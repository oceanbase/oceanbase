/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "lib/timezone/ob_time_convert.h"
#include "sql/hybrid_search/ob_hybrid_search_dsl_resolver.h"
#include "sql/hybrid_search/ob_fulltext_search_query.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "lib/utility/ob_fast_convert.h"
#include "sql/rewrite/ob_expand_aggregate_utils.h"
#include "share/vector_index/ob_vector_index_util.h"

namespace oceanbase
{
namespace sql
{

const ObString ObDSLResolver::FTS_SCORE_NAME("__fts_score");
const ObString ObDSLResolver::VS_SCORE_PREFIX("__vs_score_");

int ObDSLQueryInfo::deep_copy_sort_items(const ObDSLQueryInfo &src, ObIRawExprCopier &expr_copier)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < src.sort_items_.count(); ++i) {
    const ObDSLSortItem &src_sort_item = src.sort_items_.at(i);
    ObDSLSortItem dst_sort_item;
    ObRawExpr *copied_missing_literal = nullptr;
    if (OB_FAIL(expr_copier.copy(src_sort_item.field_expr_, dst_sort_item.field_expr_))) {
      LOG_WARN("failed to copy sort field expr", K(ret), K(i));
    } else if (OB_FALSE_IT(dst_sort_item.is_asc_ = src_sort_item.is_asc_)) {
    } else if (OB_FALSE_IT(dst_sort_item.missing_mode_ = src_sort_item.missing_mode_)) {
    } else if (OB_FAIL(expr_copier.copy(static_cast<ObRawExpr *>(src_sort_item.missing_literal_), copied_missing_literal))) {
      LOG_WARN("failed to copy sort missing literal", K(ret), K(i));
    } else if (OB_FALSE_IT(dst_sort_item.missing_literal_ = static_cast<ObConstRawExpr *>(copied_missing_literal))) {
    } else if (OB_FALSE_IT(dst_sort_item.is_score_sort_ = src_sort_item.is_score_sort_)) {
    } else if (OB_FAIL(sort_items_.push_back(dst_sort_item))) {
      LOG_WARN("failed to push back sort item", K(ret), K(i));
    }
  }
  return ret;
}

int ObDSLResolver::resolve_hybrid_search_score_column_ref_expr(
    const TableItem &table_item,
    const ObQualifiedName &q_name,
    ObDMLStmt &stmt,
    ObRawExpr *&real_ref_expr)
{
  int ret = OB_SUCCESS;
  if (0 != q_name.col_name_.case_compare(OB_HYBRID_SEARCH_SCORE_COLUMN_NAME)) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_WARN("not a hybrid search score column", K(ret), K(q_name));
  } else {
    const ObDSLQueryInfo *dsl_query = table_item.dsl_query_;
    ObOpPseudoColumnRawExpr *score_expr = nullptr;
    int32_t rel_id = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(score_expr) && i < dsl_query->score_cols_.count(); ++i) {
      ObOpPseudoColumnRawExpr *tmp = dsl_query->score_cols_.at(i);
      if (OB_ISNULL(tmp) || OB_ISNULL(tmp->get_name())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hybrid search score expr is null or has no name", K(ret), K(i));
      } else if (0 == ObString::make_string(tmp->get_name()).case_compare(OB_HYBRID_SEARCH_SCORE_COLUMN_NAME)) {
        score_expr = tmp;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(score_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hybrid search score expr not found", K(ret), K(q_name));
    } else if (OB_FALSE_IT(rel_id = stmt.get_table_bit_index(table_item.table_id_))) {
    } else if (OB_FAIL(score_expr->add_relation_id(rel_id))) {
      LOG_WARN("failed to add relation id for hybrid search score", K(ret), K(rel_id));
    } else {
      real_ref_expr = score_expr;
    }
  }
  return ret;
}

int ObDSLResolver::resolve_hybrid_search_pseudo_column_ref_expr(
    TableItem &table_item,
    const ObQualifiedName &q_name,
    ObDMLStmt &stmt,
    ObRawExprFactory &expr_factory,
    ObSQLSessionInfo *session_info,
    ObRawExpr *&real_ref_expr)
{
  int ret = OB_SUCCESS;
  real_ref_expr = nullptr;
  if (!table_item.is_hybrid_search_table()) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_WARN("not a hybrid search table", K(ret), K(table_item));
  } else if (0 == q_name.col_name_.case_compare(OB_HYBRID_SEARCH_SCORE_COLUMN_NAME)) {
    if (OB_FAIL(resolve_hybrid_search_score_column_ref_expr(table_item, q_name, stmt, real_ref_expr))) {
      LOG_WARN("failed to resolve hybrid search score column", K(ret), K(q_name));
    }
  } else if (OB_FAIL(resolve_hybrid_search_aggs_bucket_column_ref_expr(table_item, q_name, stmt,
                                                                       expr_factory, session_info, real_ref_expr))) {
    if (OB_ERR_BAD_FIELD_ERROR != ret) {
      LOG_WARN("failed to resolve hybrid search aggs bucket column", K(ret), K(q_name));
    }
  }
  // add new pseudo column resolvers here
  return ret;
}

int ObDSLResolver::find_aggs_bucket_item(
    const TableItem &table_item,
    const ObQualifiedName &q_name,
    ObDSLAggTermsItem *&bucket_item)
{
  int ret = OB_SUCCESS;
  bucket_item = nullptr;
  ObIArray<ObDSLAggTermsItem> &agg_items = table_item.dsl_query_->agg_items_;
  for (int64_t i = 0; OB_SUCC(ret) && NULL == bucket_item && i < agg_items.count(); ++i) {
    if (0 == q_name.col_name_.case_compare(agg_items.at(i).agg_name_)) {
      bucket_item = &agg_items.at(i);
    }
  }
  if (OB_ISNULL(bucket_item)) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_WARN("column is not a hybrid_search aggs bucket name", K(ret), K(q_name));
  }
  return ret;
}

int ObDSLResolver::resolve_terms_bucket_expr(
    ObDSLAggTermsItem &bucket_item,
    ObRawExpr *&real_ref_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bucket_item.count_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("terms agg count expr not injected yet", K(ret));
  } else {
    real_ref_expr = bucket_item.count_expr_;
  }
  return ret;
}

int ObDSLResolver::resolve_cardinality_bucket_expr(
    ObDSLAggTermsItem &bucket_item,
    ObDMLStmt &stmt,
    ObRawExprFactory &expr_factory,
    ObSQLSessionInfo *session_info,
    ObRawExpr *&real_ref_expr)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(bucket_item.count_expr_)) {
    real_ref_expr = bucket_item.count_expr_;
  } else {
    ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(&stmt);
    ObAggFunRawExpr *agg_expr = nullptr;
    if (OB_ISNULL(bucket_item.field_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cardinality field is null", K(ret));
    } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_COUNT, agg_expr))) {
      LOG_WARN("fail to create count expr", K(ret));
    } else if (OB_FAIL(agg_expr->add_real_param_expr(bucket_item.field_))) {
      LOG_WARN("fail to add real param for count distinct", K(ret));
    } else if (OB_FALSE_IT(agg_expr->set_param_distinct(true))) {
    } else if (OB_FAIL(agg_expr->formalize(session_info))) {
      LOG_WARN("formalize cardinality count expr failed", K(ret));
    } else {
      ObAggFunRawExpr *same_aggr = nullptr;
      if (OB_FAIL(select_stmt->check_and_get_same_aggr_item(agg_expr, same_aggr))) {
        LOG_WARN("check same aggr failed", K(ret));
      } else if (OB_NOT_NULL(same_aggr)) {
        bucket_item.count_expr_ = same_aggr;
        real_ref_expr = same_aggr;
      } else if (OB_FAIL(select_stmt->add_agg_item(*agg_expr))) {
        LOG_WARN("add agg item for cardinality failed", K(ret));
      } else {
        bucket_item.count_expr_ = agg_expr;
        real_ref_expr = agg_expr;
      }
    }
  }
  return ret;
}

int ObDSLResolver::resolve_hybrid_search_aggs_bucket_column_ref_expr(
    TableItem &table_item,
    const ObQualifiedName &q_name,
    ObDMLStmt &stmt,
    ObRawExprFactory &expr_factory,
    ObSQLSessionInfo *session_info,
    ObRawExpr *&real_ref_expr)
{
  int ret = OB_SUCCESS;
  real_ref_expr = nullptr;
  ObDSLAggTermsItem *bucket_item = nullptr;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(find_aggs_bucket_item(table_item, q_name, bucket_item))) {
    if (OB_ERR_BAD_FIELD_ERROR != ret) {
      LOG_WARN("failed to find aggs bucket item", K(ret), K(q_name));
    }
  } else if (bucket_item->agg_type_ == ObDSLAggTermsItem::TERMS) {
    if (OB_FAIL(resolve_terms_bucket_expr(*bucket_item, real_ref_expr))) {
      LOG_WARN("fail to resolve terms bucket", K(ret));
    }
  } else if (bucket_item->agg_type_ == ObDSLAggTermsItem::CARDINALITY) {
    if (OB_FAIL(resolve_cardinality_bucket_expr(*bucket_item, stmt, expr_factory, session_info, real_ref_expr))) {
      LOG_WARN("fail to resolve cardinality bucket", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected agg type for bucket column", K(ret), K(bucket_item->agg_type_));
  }
  return ret;
}

int ObDSLResolver::add_hybrid_search_score_to_select_list(
    const TableItem &table_item,
    ObDMLStmt &stmt,
    common::ObIArray<SelectItem> &target_list)
{
  int ret = OB_SUCCESS;

  if (!table_item.is_hybrid_search_table()) {
    // not a hybrid search table, do nothing
  } else {
    const ObDSLQueryInfo *dsl_query = table_item.dsl_query_;
    ObOpPseudoColumnRawExpr *score_expr = nullptr;
    bool already_has_score = false;

    // Check if `score` already exists in target_list
    for (int64_t i = 0; !already_has_score && i < target_list.count(); ++i) {
      already_has_score = 0 == target_list.at(i).alias_name_.case_compare(OB_HYBRID_SEARCH_SCORE_COLUMN_NAME);
    }

    if (already_has_score) {
      // skip, already has score
    } else if (OB_ISNULL(dsl_query)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hybrid search table has null dsl_query_", K(ret), K(table_item));
    } else if (dsl_query->score_cols_.empty()) {
      // no score columns, do nothing
    } else if (OB_ISNULL(score_expr = dsl_query->score_cols_.at(dsl_query->score_cols_.count() - 1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hybrid search score expr is null", K(ret), K(dsl_query->score_cols_.count()));
    } else {
      // bind relation id to current table for correctness in later phases
      const int64_t rel_id = stmt.get_table_bit_index(table_item.table_id_);
      if (OB_FAIL(score_expr->add_relation_id(rel_id))) {
        LOG_WARN("failed to add relation id for hybrid search score", K(ret), K(rel_id));
      } else {
        SelectItem score_item;
        score_item.alias_name_ = ObString::make_string(OB_HYBRID_SEARCH_SCORE_COLUMN_NAME);
        score_item.expr_name_ = ObString::make_string(OB_HYBRID_SEARCH_SCORE_COLUMN_NAME);
        score_item.is_real_alias_ = false;
        score_item.expr_ = score_expr;
        if (OB_FAIL(target_list.push_back(score_item))) {
          LOG_WARN("failed to push back hybrid search score select item", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObDSLBoolQuery::create(ObIAllocator &alloc, ObDSLBoolQuery *&bool_query,
                           ObEsQueryItem outer_query_type, ObDSLQuery *parent_query)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(bool_query = OB_NEWx(ObDSLBoolQuery, &alloc, outer_query_type, parent_query))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObDSLBoolQuery", K(ret));
  }
  return ret;
}

int ObDSLKnnQuery::create(ObIAllocator &alloc, ObDSLKnnQuery *&knn_query, ObEsQueryItem outer_query_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(knn_query = OB_NEWx(ObDSLKnnQuery, &alloc, outer_query_type))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObDSLKnnQuery", K(ret));
  }
  return ret;
}

int ObDSLScalarQuery::create(ObIAllocator &alloc, ObDSLScalarQuery *&scalar_query,
                             ObEsQueryItem query_type, ObEsQueryItem outer_query_type, ObDSLQuery *parent_query)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scalar_query = OB_NEWx(ObDSLScalarQuery, &alloc, query_type, outer_query_type, parent_query))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObDSLScalarQuery", K(ret));
  }
  return ret;
}

int ObDSLQueryInfo::check_column_in_dsl(ObIArray<TableItem*> &table_items, ObColumnRefRawExpr *col_expr, bool &in_dsl)
{
  int ret = OB_SUCCESS;
  int64_t table_size = table_items.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < table_size; ++i) {
    const TableItem *table_item = table_items.at(i);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (table_item->is_hybrid_search_table()) {
      const ObDSLQueryInfo *dsl_query_info = table_item->dsl_query_;
      if (is_contain(dsl_query_info->dsl_cols, col_expr)) {
        in_dsl = true;
        break;
      }
    }
  }
  return ret;
}

int ObDSLQueryInfo::deep_copy(const ObDSLQueryInfo& src, ObIRawExprCopier &expr_copier, ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  ObRawExpr *copied_collapse_field = nullptr;
  ObRawExpr *copied_win_func_expr = nullptr;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param, invalid param", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src.from_, from_))) {
    LOG_WARN("failed to copy from expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src.size_, size_))) {
    LOG_WARN("failed to copy size expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src.min_score_, min_score_))) {
    LOG_WARN("failed to copy min score expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src.query_top_level_boost_, query_top_level_boost_))) {
    LOG_WARN("failed to copy query top level boost expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src.rowkey_cols_, rowkey_cols_))) {
    LOG_WARN("failed to copy rowkey cols", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src.dsl_cols, dsl_cols))) {
    LOG_WARN("failed to copy scalar query cols", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src.score_cols_, score_cols_))) {
    LOG_WARN("failed to copy score cols", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src.dsl_exprs_, dsl_exprs_))) {
    LOG_WARN("failed to copy dsl exprs", K(ret));
  } else if (OB_FAIL(deep_copy_sort_items(src, expr_copier))) {
    LOG_WARN("failed to deep copy sort items", K(ret));
  } else if (OB_FALSE_IT(rank_info_.method_ = src.rank_info_.method_)) {
  } else if (OB_FALSE_IT(rank_info_.has_rank_ = src.rank_info_.has_rank_)) {
  } else if (OB_FAIL(expr_copier.copy(src.rank_info_.window_size_, rank_info_.window_size_))) {
    LOG_WARN("failed to copy window_size expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src.rank_info_.rank_const_, rank_info_.rank_const_))) {
    LOG_WARN("failed to copy rank_const expr", K(ret));
  } else if (OB_FAIL(ob_write_string(*allocator, src.raw_dsl_param_str_, raw_dsl_param_str_))) {
    LOG_WARN("failed to copy raw dsl param string", K(ret));
  } else if (OB_FALSE_IT(query_dop_ = src.query_dop_)) {
  } else if (OB_FAIL(expr_copier.copy(src.collapse_info_.field_, copied_collapse_field))) {
    LOG_WARN("failed to copy collapse field", K(ret));
  } else if (OB_FALSE_IT(collapse_info_.field_ = static_cast<ObColumnRefRawExpr*>(copied_collapse_field))) {
  } else if (OB_FAIL(expr_copier.copy(static_cast<ObRawExpr*>(src.collapse_info_.win_func_expr_), copied_win_func_expr))) {
    LOG_WARN("failed to copy collapse win_func_expr", K(ret));
  } else if (OB_FALSE_IT(collapse_info_.win_func_expr_ = static_cast<ObWinFunRawExpr*>(copied_win_func_expr))) {
  } else if (OB_FAIL(expr_copier.copy(src.collapse_info_.qualify_expr_, collapse_info_.qualify_expr_))) {
    LOG_WARN("failed to copy collapse qualify_expr", K(ret));
  } else if (OB_FALSE_IT(is_top_k_query_ = src.is_top_k_query_)) {
  } else if (OB_FALSE_IT(result_mode_ = src.result_mode_)) {
  } else if (OB_FALSE_IT(track_score_ = src.track_score_)) {
  } else if (OB_FALSE_IT(output_score_ = src.output_score_)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < src.queries_.count(); ++i) {
      ObDSLQuery *copied_query = nullptr;
      if (OB_ISNULL(src.queries_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("source query is null", K(ret), K(i));
      } else if (OB_FAIL(deep_copy_query(src.queries_.at(i), copied_query, expr_copier, allocator))) {
        LOG_WARN("failed to deep copy query", K(ret), K(i));
      } else if (OB_FAIL(queries_.push_back(copied_query))) {
        LOG_WARN("failed to push back copied query", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < src.agg_items_.count(); ++i) {
      ObDSLAggTermsItem copied;
      ObRawExpr *field_raw = nullptr;
      ObRawExpr *count_raw = nullptr;
      if (OB_FAIL(ob_write_string(*allocator, src.agg_items_.at(i).agg_name_, copied.agg_name_))) {
        LOG_WARN("failed to copy agg name", K(ret), K(i));
      } else if (OB_FAIL(expr_copier.copy(static_cast<const ObRawExpr *>(src.agg_items_.at(i).field_), field_raw))) {
        LOG_WARN("failed to copy agg field", K(ret), K(i));
      } else if (OB_FAIL(expr_copier.copy(static_cast<const ObRawExpr *>(src.agg_items_.at(i).count_expr_), count_raw))) {
        LOG_WARN("failed to copy agg count_expr", K(ret), K(i));
      } else {
        copied.agg_type_ = src.agg_items_.at(i).agg_type_;
        copied.field_ = static_cast<ObColumnRefRawExpr *>(field_raw);
        copied.count_expr_ = static_cast<ObAggFunRawExpr *>(count_raw);
        copied.size_ = src.agg_items_.at(i).size_;
        copied.min_doc_count_ = src.agg_items_.at(i).min_doc_count_;
        copied.order_by_ = src.agg_items_.at(i).order_by_;
        copied.order_asc_ = src.agg_items_.at(i).order_asc_;
        if (OB_FAIL(agg_items_.push_back(copied))) {
          LOG_WARN("failed to push back copied agg item", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObDSLQueryInfo::deep_copy_query(const ObDSLQuery *src, ObDSLQuery *&dst,
                                    ObIRawExprCopier &expr_copier, ObIAllocator* allocator, ObDSLQuery *parent_query)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src) || OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", K(ret), KP(src), KP(allocator));
  } else if ((OB_ISNULL(src->parent_query_) != OB_ISNULL(parent_query))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parent and parent of source are not both null or both non-null", K(ret), KP(src->parent_query_), KP(parent_query));
  } else if (IS_QUERY_ITEM_FULLTEXT(src->query_type_)) {
    const ObDSLFullTextQuery *src_fulltext = static_cast<const ObDSLFullTextQuery *>(src);
    ObDSLFullTextQuery *dst_fulltext = nullptr;
    if (OB_FAIL(deep_copy_query_fulltext(src_fulltext, dst_fulltext, expr_copier, allocator, parent_query))) {
      LOG_WARN("failed to deep copy match query", K(ret));
    } else {
      dst = dst_fulltext;
    }
  } else if (IS_QUERY_ITEM_BOOL(src->query_type_)) {
    const ObDSLBoolQuery *src_bool = static_cast<const ObDSLBoolQuery *>(src);
    ObDSLBoolQuery *dst_bool = nullptr;
    if (OB_FAIL(deep_copy_query_bool(src_bool, dst_bool, expr_copier, allocator, parent_query))) {
      LOG_WARN("failed to deep copy bool query", K(ret));
    } else {
      dst = dst_bool;
    }
  } else if (IS_QUERY_ITEM_KNN(src->query_type_)) {
    const ObDSLKnnQuery *src_knn = static_cast<const ObDSLKnnQuery *>(src);
    ObDSLKnnQuery *dst_knn = nullptr;
    if (OB_FAIL(deep_copy_query_knn(src_knn, dst_knn, expr_copier, allocator))) {
      LOG_WARN("failed to deep copy knn query", K(ret));
    } else {
      dst = dst_knn;
    }
  } else if (IS_QUERY_ITEM_SCALAR(src->query_type_) || IS_QUERY_ITEM_JSON(src->query_type_) || IS_QUERY_ITEM_ARRAY(src->query_type_)) {
    const ObDSLScalarQuery *src_scalar = static_cast<const ObDSLScalarQuery *>(src);
    ObDSLScalarQuery *dst_scalar = nullptr;
    if (OB_FAIL(deep_copy_query_scalar(src_scalar, dst_scalar, expr_copier, allocator, parent_query))) {
      LOG_WARN("failed to deep copy scalar query", K(ret));
    } else {
      dst = dst_scalar;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported query type for deep copy", K(ret), K(src->query_type_));
  }
  if (OB_FAIL(ret)) {
  } else {
    ObRawExpr *boost_expr = nullptr;
    if (OB_FAIL(expr_copier.copy(src->boost_, boost_expr))) {
      LOG_WARN("failed to copy boost expr", K(ret));
    } else if (OB_FALSE_IT(dst->boost_ = static_cast<ObConstRawExpr*>(boost_expr))) {
    } else {
      dst->assign_common_attr(src);
    }
  }
  return ret;
}

int ObDSLQueryInfo::deep_copy_query_bool(const ObDSLBoolQuery *src, ObDSLBoolQuery *&dst,
                                         ObIRawExprCopier &expr_copier, ObIAllocator* allocator, ObDSLQuery *parent)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDSLQuery*, 4, ModulePageAllocator, true> must;
  ObSEArray<ObDSLQuery*, 4, ModulePageAllocator, true> should;
  ObSEArray<ObDSLQuery*, 4, ModulePageAllocator, true> filter;
  ObSEArray<ObDSLQuery*, 4, ModulePageAllocator, true> must_not;
  if (OB_ISNULL(src) || OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", K(ret), KP(src), KP(allocator));
  } else if (OB_FAIL(ObDSLBoolQuery::create(*allocator, dst, src->outer_query_type_, parent))) {
    LOG_WARN("failed to create bool query", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < src->must_.count(); ++i) {
    ObDSLQuery *copied_query = nullptr;
    if (OB_FAIL(deep_copy_query(src->must_.at(i), copied_query, expr_copier, allocator, dst))) {
      LOG_WARN("failed to deep copy must query", K(ret));
    } else if (OB_FAIL(must.push_back(copied_query))) {
      LOG_WARN("failed to push back must query", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < src->should_.count(); ++i) {
    ObDSLQuery *copied_query = nullptr;
    if (OB_FAIL(deep_copy_query(src->should_.at(i), copied_query, expr_copier, allocator, dst))) {
      LOG_WARN("failed to deep copy should query", K(ret));
    } else if (OB_FAIL(should.push_back(copied_query))) {
      LOG_WARN("failed to push back should query", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < src->filter_.count(); ++i) {
    ObDSLQuery *copied_query = nullptr;
    if (OB_FAIL(deep_copy_query(src->filter_.at(i), copied_query, expr_copier, allocator, dst))) {
      LOG_WARN("failed to deep copy filter query", K(ret));
    } else if (OB_FAIL(filter.push_back(copied_query))) {
      LOG_WARN("failed to push back filter query", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < src->must_not_.count(); ++i) {
    ObDSLQuery *copied_query = nullptr;
    if (OB_FAIL(deep_copy_query(src->must_not_.at(i), copied_query, expr_copier, allocator, dst))) {
      LOG_WARN("failed to deep copy must_not query", K(ret));
    } else if (OB_FAIL(must_not.push_back(copied_query))) {
      LOG_WARN("failed to push back must_not query", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dst->must_.assign(must))) {
    LOG_WARN("failed to assign must queries", K(ret));
  } else if (OB_FAIL(dst->should_.assign(should))) {
    LOG_WARN("failed to assign should queries", K(ret));
  } else if (OB_FAIL(dst->filter_.assign(filter))) {
    LOG_WARN("failed to assign filter queries", K(ret));
  } else if (OB_FAIL(dst->must_not_.assign(must_not))) {
    LOG_WARN("failed to assign must_not queries", K(ret));
  } else {
    ObRawExpr *msm_copy = nullptr;
    dst->must_cnt_ = src->must_cnt_;
    dst->should_cnt_ = src->should_cnt_;
    dst->filter_cnt_ = src->filter_cnt_;
    dst->must_not_cnt_ = src->must_not_cnt_;
    dst->msm_ = src->msm_;
    if (OB_FAIL(expr_copier.copy(src->minimum_should_match_, msm_copy))) {
      LOG_WARN("failed to copy bool minimum_should_match expr", K(ret));
    } else {
      dst->minimum_should_match_ = static_cast<ObConstRawExpr *>(msm_copy);
    }
  }
  return ret;
}

int ObDSLQueryInfo::deep_copy_query_knn(const ObDSLKnnQuery *src, ObDSLKnnQuery *&dst,
                                        ObIRawExprCopier &expr_copier, ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDSLQuery*, 4, ModulePageAllocator, true> filter;
  ObRawExpr *field_expr = nullptr;
  ObRawExpr *k_expr = nullptr;
  ObRawExpr *query_vector_expr = nullptr;
  ObRawExpr *distance_expr = nullptr;
  ObDSLKnnQuery::SearchOption *search_option = nullptr;
  if (OB_ISNULL(src) || OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", K(ret), KP(src), KP(allocator));
  } else if (OB_FAIL(expr_copier.copy(src->field_, field_expr))) {
    LOG_WARN("failed to copy field", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src->k_, k_expr))) {
    LOG_WARN("failed to copy k", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src->query_vector_, query_vector_expr))) {
    LOG_WARN("failed to copy query vector", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src->distance_, distance_expr))) {
    LOG_WARN("failed to copy distance", K(ret));
  } else if (OB_NOT_NULL(src->search_option_) &&
             OB_ISNULL(search_option = OB_NEWx(ObDSLKnnQuery::SearchOption, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate search option", K(ret));
  } else if (OB_FAIL(ObDSLKnnQuery::create(*allocator, dst, src->outer_query_type_))) {
    LOG_WARN("failed to create knn query", K(ret));
  } else if (OB_NOT_NULL(search_option)) {
    *search_option = *src->search_option_;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < src->filter_.count(); ++i) {
    ObDSLQuery *copied_query = nullptr;
    if (OB_FAIL(deep_copy_query(src->filter_.at(i), copied_query, expr_copier, allocator, dst))) {
      LOG_WARN("failed to deep copy filter query", K(ret));
    } else if (OB_FAIL(filter.push_back(copied_query))) {
      LOG_WARN("failed to push back filter query", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dst->filter_.assign(filter))) {
    LOG_WARN("failed to assign filter queries", K(ret));
  } else {
    dst->dist_algo_ = src->dist_algo_;
    dst->field_ = static_cast<ObColumnRefRawExpr*>(field_expr);
    dst->k_ = static_cast<ObConstRawExpr*>(k_expr);
    dst->query_vector_ = static_cast<ObConstRawExpr*>(query_vector_expr);
    dst->distance_ = distance_expr;
    dst->search_option_ = search_option;
  }
  return ret;
}

int ObDSLQueryInfo::deep_copy_query_fulltext(
    const ObDSLFullTextQuery *src,
    ObDSLFullTextQuery *&dst,
    ObIRawExprCopier &expr_copier,
    ObIAllocator* allocator,
    ObDSLQuery *parent)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src) || OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", K(ret), KP(src), KP(allocator));
  } else if (OB_FAIL(src->deep_copy(parent, expr_copier, *allocator, dst))) {
    LOG_WARN("failed to deep copy fulltext query", K(ret));
  }
  return ret;
}

int ObDSLQueryInfo::deep_copy_query_scalar(const ObDSLScalarQuery *src, ObDSLScalarQuery *&dst,
                                           ObIRawExprCopier &expr_copier, ObIAllocator* allocator, ObDSLQuery *parent)
{
  int ret = OB_SUCCESS;
  ObRawExpr *field_expr = nullptr;
  ObRawExpr *scalar_expr = nullptr;
  if (OB_ISNULL(src) || OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", K(ret), KP(src), KP(allocator));
  } else if (OB_FAIL(expr_copier.copy(src->field_, field_expr))) {
    LOG_WARN("failed to copy field", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src->scalar_expr_, scalar_expr))) {
    LOG_WARN("failed to copy scalar expr", K(ret));
  } else if (OB_FAIL(ObDSLScalarQuery::create(*allocator, dst, src->query_type_, src->outer_query_type_, parent))) {
    LOG_WARN("failed to create scalar query", K(ret));
  } else {
    dst->field_ = field_expr;
    dst->scalar_expr_ = scalar_expr;
  }
  return ret;
}

int ObDSLQueryInfo::init_default_params(ObRawExprFactory &expr_factory, bool is_top_k_query)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *one_const_expr = nullptr;
  ObConstRawExpr *from_expr = nullptr;
  ObConstRawExpr *size_expr = nullptr;
  ObConstRawExpr *min_score_expr = nullptr;
  if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, ObDSLResolver::FROM_DEFAULT, from_expr))) {
    LOG_WARN("failed to create from const expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, ObDSLResolver::SIZE_DEFAULT, size_expr))) {
    LOG_WARN("failed to create size const expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_double_expr(expr_factory, ObDoubleType, ObDSLResolver::MIN_SCORE_DEFAULT, min_score_expr))) {
    LOG_WARN("failed to create min score const expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_double_expr(expr_factory, ObDoubleType, 1.0, one_const_expr))) {
    LOG_WARN("failed to create default boost const expr", K(ret));
  } else {
    from_ = from_expr;
    size_ = size_expr;
    min_score_ = min_score_expr;
    rank_info_.method_ = ObFusionMethod::WEIGHT_SUM;
    rank_info_.window_size_ = size_expr;
    one_const_expr_ = one_const_expr;
    query_top_level_boost_ = one_const_expr;
    is_top_k_query_ = is_top_k_query;
  }
  return ret;
}

int ObDSLResolver::add_dsl_expr(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(dsl_query_info_->dsl_exprs_.push_back(expr))) {
    LOG_WARN("fail to add dsl expr", K(ret));
  }
  return ret;
}

int ObDSLResolver::add_dsl_expr_recursive(ObDSLQuery *query)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query is null", K(ret));
  } else if (query->boost_ != dsl_query_info_->one_const_expr_ &&
             OB_FAIL(add_dsl_expr(query->boost_))) {
    LOG_WARN("fail to add boost expr", K(ret));
  } else if (IS_QUERY_ITEM_BOOL(query->query_type_)) {
    ObDSLBoolQuery *bool_query = static_cast<ObDSLBoolQuery*>(query);
    if (OB_NOT_NULL(bool_query->minimum_should_match_) &&
        OB_FAIL(add_dsl_expr(bool_query->minimum_should_match_))) {
      LOG_WARN("fail to add bool minimum_should_match expr", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < bool_query->must_.count(); i++) {
      if (OB_FAIL(add_dsl_expr_recursive(bool_query->must_.at(i)))) {
        LOG_WARN("fail to add must query expr", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < bool_query->should_.count(); i++) {
      if (OB_FAIL(add_dsl_expr_recursive(bool_query->should_.at(i)))) {
        LOG_WARN("fail to add should query expr", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < bool_query->filter_.count(); i++) {
      if (OB_FAIL(add_dsl_expr_recursive(bool_query->filter_.at(i)))) {
        LOG_WARN("fail to add filter query expr", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < bool_query->must_not_.count(); i++) {
      if (OB_FAIL(add_dsl_expr_recursive(bool_query->must_not_.at(i)))) {
        LOG_WARN("fail to add must_not query expr", K(ret));
      }
    }
  } else if (IS_QUERY_ITEM_SCALAR(query->query_type_) ||
             IS_QUERY_ITEM_JSON(query->query_type_) ||
             IS_QUERY_ITEM_ARRAY(query->query_type_)) {
    ObDSLScalarQuery *scalar_query = static_cast<ObDSLScalarQuery*>(query);
    if (OB_FAIL(add_dsl_expr(scalar_query->scalar_expr_))) {
      LOG_WARN("fail to add scalar expr", K(ret));
    }
  } else if (IS_QUERY_ITEM_KNN(query->query_type_)) {
    ObDSLKnnQuery *knn_query = static_cast<ObDSLKnnQuery*>(query);
    if (OB_FAIL(add_dsl_expr(knn_query->k_))) {
      LOG_WARN("fail to add k expr", K(ret));
    } else if (OB_FAIL(add_dsl_expr(knn_query->distance_))) {
      LOG_WARN("fail to add distance expr", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < knn_query->filter_.count(); i++) {
      if (OB_FAIL(add_dsl_expr_recursive(knn_query->filter_.at(i)))) {
        LOG_WARN("fail to add filter query expr", K(ret), K(i));
      }
    }
  } else if (IS_QUERY_ITEM_FULLTEXT(query->query_type_)) {
    ObDSLFullTextQuery *fulltext_query = static_cast<ObDSLFullTextQuery*>(query);
    if (OB_FAIL(fulltext_query->collect_exprs(dsl_query_info_->dsl_exprs_))) {
      LOG_WARN("fail to collect exprs", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported query type for add dsl expr", K(ret), K(query->query_type_));
  }
  return ret;
}

int ObDSLResolver::build_array_intersects_expr(ObColumnRefRawExpr *col_expr,
                                               const ObIArray<ObRawExpr*> &value_exprs,
                                               ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  const ObSqlCollectionInfo *coll_info = nullptr;
  ObSysFunRawExpr *func_expr = nullptr;
  if (OB_FAIL(ObRawExprUtils::get_expr_collection_info(col_expr, session_info_->get_cur_exec_ctx(), coll_info))) {
    LOG_WARN("fail to get collection meta for column", K(ret), KPC(col_expr));
  } else if (OB_ISNULL(coll_info) || OB_ISNULL(coll_info->collection_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("collection info is null", K(ret), KPC(col_expr));
  } else if (coll_info->collection_meta_->type_id_ != ObNestedType::OB_ARRAY_TYPE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column is not an array type", K(ret), KPC(col_expr));
  } else {
    uint32_t nest_depth = 0;
    UNUSED(coll_info->get_basic_meta(nest_depth));
    if (nest_depth > 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "term/terms query on nested array column");
      LOG_WARN("term/terms query on nested array column not supported", K(ret), K(nest_depth), KPC(col_expr));
    }
  }
  // to get the intersection of column array and values:
  //   for single value, build ARRAY_CONTAINS(col, scalar) with better execution performance
  //   for multiple values, build ARRAY_OVERLAPS(col, array)
  if (OB_FAIL(ret)) {
  } else if (value_exprs.count() == 1) {
    if (OB_FAIL(expr_factory->create_raw_expr(T_FUNC_SYS_ARRAY_CONTAINS, func_expr))) {
      LOG_WARN("fail to create array contains expr", K(ret));
    } else if (OB_FALSE_IT(func_expr->set_func_name(N_ARRAY_CONTAINS))) {
    } else if (OB_FAIL(func_expr->set_param_exprs(col_expr, value_exprs.at(0)))) {
      LOG_WARN("fail to set param exprs for array contains", K(ret));
    } else {
      expr = func_expr;
    }
  } else {
    ObSysFunRawExpr *array_expr = nullptr;
    if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SYS_ARRAY, array_expr))) {
      LOG_WARN("fail to create array expr", K(ret));
    } else if (OB_FALSE_IT(array_expr->set_func_name(N_ARRAY))) {
    } else if (OB_FAIL(array_expr->init_param_exprs(value_exprs.count()))) {
      LOG_WARN("fail to init param exprs for array", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < value_exprs.count(); ++i) {
        if (OB_FAIL(array_expr->add_param_expr(value_exprs.at(i)))) {
          LOG_WARN("fail to add param expr to array", K(ret), K(i));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUNC_SYS_ARRAY_OVERLAPS, func_expr))) {
      LOG_WARN("fail to create array overlaps expr", K(ret));
    } else if (OB_FALSE_IT(func_expr->set_func_name(N_ARRAY_OVERLAPS))) {
    } else if (OB_FAIL(func_expr->set_param_exprs(col_expr, array_expr))) {
      LOG_WARN("fail to set param exprs for array overlaps", K(ret));
    } else {
      expr = func_expr;
    }
  }
  return ret;
}

int ObDSLResolver::build_const_double(double dval, ObConstRawExpr *&const_expr, ObObjType array_base_type)
{
  int ret = OB_SUCCESS;
  if (array_base_type == ObFloatType && !ObArithExprOperator::is_float_out_of_range(static_cast<float>(dval))) {
    if (OB_FAIL(ObRawExprUtils::build_const_float_expr(*params_.expr_factory_, ObFloatType, static_cast<float>(dval), const_expr))) {
      LOG_WARN("fail to create const float expr", K(ret), K(dval));
    }
  } else if (dval == 1.0) {
    const_expr = static_cast<ObConstRawExpr*>(dsl_query_info_->one_const_expr_);
  } else if (OB_FAIL(ObRawExprUtils::build_const_double_expr(*params_.expr_factory_, ObDoubleType, dval, const_expr))) {
    LOG_WARN("fail to create const double expr", K(ret), K(dval));
  }
  return ret;
}

int ObDSLResolver::build_field_expr_with_path(ObColumnRefRawExpr *col_expr, const ObString &path_str, const ObEsQueryItem query_type, ObRawExpr *&field_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path_str.ptr()) ||
      (query_type == QUERY_ITEM_JSON_MEMBER_OF && path_str.compare_equal("$"))) {
    // use column expr without json_extract if any of the following conditions is met:
    // (1) no path
    // (2) json member of with root array path
    if (!col_expr->get_result_type().is_json() && OB_FAIL(dsl_query_info_->dsl_cols.push_back(col_expr))) {
      LOG_WARN("failed to push back scalar query column expr", K(ret));
    } else {
      field_expr = col_expr;
    }
  } else if (OB_FAIL(build_json_extract_expr(col_expr, path_str, field_expr))) {
    LOG_WARN("fail to build json_extract expr", K(ret), K(path_str), K(query_type));
  }
  return ret;
}

int ObDSLResolver::build_json_contains_scalar_expr(ObRawExpr *target_expr, ObIJsonBase &json_node, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  ObJsonBuffer j_buf(allocator_);
  ObString candidate_str;
  ObConstRawExpr *candidate_expr = nullptr;
  if (OB_FAIL(print_json_node(json_node, j_buf))) {
    LOG_WARN("fail to build json_contains candidate text from dsl json node", K(ret));
  } else if (OB_FAIL(j_buf.get_result_string(candidate_str))) {
    LOG_WARN("fail to get json candidate string", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*expr_factory,
                                                             ObVarcharType,
                                                             candidate_str,
                                                             CS_TYPE_UTF8MB4_BIN,
                                                             candidate_expr))) {
    LOG_WARN("fail to build const string expr for json_contains candidate", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_json_contains_expr(*expr_factory, *session_info_, target_expr, candidate_expr, expr))) {
    LOG_WARN("fail to build json_contains expr", K(ret));
  }
  return ret;
}

int ObDSLResolver::build_json_extract_expr(ObRawExpr *col_expr, const ObString &json_path, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  ObRawExpr *path_expr = nullptr;
  ObSysFunRawExpr *json_extract_expr = nullptr;
  if (OB_FAIL(construct_string_expr(json_path, path_expr, col_expr->get_collation_type()))) {
    LOG_WARN("fail to create path expr", K(ret), K(json_path), K(col_expr->get_collation_type()));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SYS_JSON_EXTRACT, json_extract_expr))) {
    LOG_WARN("fail to create json_extract expr", K(ret));
  } else if (OB_FALSE_IT(json_extract_expr->set_func_name(N_JSON_EXTRACT))) {
  } else if (OB_FAIL(json_extract_expr->set_param_exprs(col_expr, path_expr))) {
    LOG_WARN("fail to set param exprs for json_extract", K(ret));
  } else {
    expr = json_extract_expr;
  }
  return ret;
}

int ObDSLResolver::build_json_overlaps_array_expr(ObRawExpr *target_expr,
                                                  const ObIArray<ObRawExpr*> &value_exprs,
                                                  ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  ObSysFunRawExpr *json_array_expr = nullptr;
  ObSysFunRawExpr *json_overlaps_expr = nullptr;
  if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SYS_JSON_ARRAY, json_array_expr))) {
    LOG_WARN("fail to create json array expr", K(ret));
  } else if (OB_FALSE_IT(json_array_expr->set_func_name(N_JSON_ARRAY))) {
  } else if (OB_FAIL(json_array_expr->init_param_exprs(value_exprs.count()))) {
    LOG_WARN("fail to init param exprs for json array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < value_exprs.count(); ++i) {
      if (OB_FAIL(json_array_expr->add_param_expr(value_exprs.at(i)))) {
        LOG_WARN("fail to add param expr to json array", K(ret), K(i));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SYS_JSON_OVERLAPS, json_overlaps_expr))) {
    LOG_WARN("fail to create json overlaps expr", K(ret));
  } else if (OB_FALSE_IT(json_overlaps_expr->set_func_name(N_JSON_OVERLAPS))) {
  } else if (OB_FAIL(json_overlaps_expr->set_param_exprs(target_expr, json_array_expr))) {
    LOG_WARN("fail to set param exprs for json overlaps", K(ret));
  } else {
    expr = json_overlaps_expr;
  }
  return ret;
}

int ObDSLResolver::check_fields_collation_types(const ObIArray<ObColumnRefRawExpr*> &fields, bool &compatible)
{
  int ret = OB_SUCCESS;
  ObCollationType collation_type = CS_TYPE_INVALID;
  compatible = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < fields.count(); ++i) {
    const ObColumnRefRawExpr *field = fields.at(i);
    if (0 == i) {
      collation_type = field->get_collation_type();
    } else if (OB_UNLIKELY(field->get_collation_type() != collation_type)) {
      compatible = false;
      break;
    }
  }
  return ret;
}

int ObDSLResolver::check_fields_parsers(const ObIArray<ObColumnRefRawExpr*> &fields, bool &compatible)
{
  int ret = OB_SUCCESS;
  ObString parser_name;
  ObString parser_properties;
  compatible = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < fields.count(); ++i) {
    ObString col_name = fields.at(i)->get_column_name();
    const ObTableSchema *index_schema = nullptr;
    if (OB_FAIL(ObCharset::tolower(CS_TYPE_UTF8MB4_GENERAL_CI, col_name, col_name, *allocator_))) {
      LOG_WARN("fail to lower column name", K(ret), K(col_name));
    } else if (OB_FAIL(get_fulltext_index_schema(col_name, index_schema))) {
      LOG_WARN("fail to get fulltext index schema", K(ret), K(col_name));
    } else if (0 == i) {
      parser_name = index_schema->get_parser_name_str();
      parser_properties = index_schema->get_parser_property_str();
    } else if (OB_UNLIKELY(index_schema->get_parser_name_str() != parser_name ||
                           index_schema->get_parser_property_str() != parser_properties)) {
      compatible = false;
      break;
    }
  }
  return ret;
}

int ObDSLResolver::collect_exprs()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_dsl_expr(dsl_query_info_->from_))) {
    LOG_WARN("fail to add from expr", K(ret));
  } else if (OB_FAIL(add_dsl_expr(dsl_query_info_->size_))) {
    LOG_WARN("fail to add size expr", K(ret));
  } else if (OB_FAIL(add_dsl_expr(dsl_query_info_->min_score_))) {
    LOG_WARN("fail to add min score expr", K(ret));
  } else if (OB_FAIL(add_dsl_expr(dsl_query_info_->rank_info_.window_size_))) {
    LOG_WARN("fail to add rank window_size expr", K(ret));
  } else if (OB_NOT_NULL(dsl_query_info_->rank_info_.rank_const_) &&
             OB_FAIL(add_dsl_expr(dsl_query_info_->rank_info_.rank_const_))) {
    LOG_WARN("fail to add rank_const expr", K(ret));
  } else if (OB_FAIL(add_dsl_expr(dsl_query_info_->one_const_expr_))) {
    LOG_WARN("fail to add one const expr", K(ret));
  } else if (OB_NOT_NULL(dsl_query_info_->query_top_level_boost_) &&
             OB_FAIL(add_dsl_expr(dsl_query_info_->query_top_level_boost_))) {
    LOG_WARN("fail to add query top level boost expr", K(ret));
  } else if (dsl_query_info_->has_dsl_collapse() &&
             OB_FAIL(add_dsl_expr(dsl_query_info_->collapse_info_.field_))) {
    LOG_WARN("fail to add collapse field expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dsl_query_info_->queries_.count(); i++) {
      if (OB_FAIL(add_dsl_expr_recursive(dsl_query_info_->queries_.at(i)))) {
        LOG_WARN("fail to add vs query root exprs", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dsl_query_info_->rowkey_cols_.count(); i++) {
      if (OB_FAIL(add_dsl_expr(dsl_query_info_->rowkey_cols_.at(i)))) {
        LOG_WARN("fail to add rowkey column expr", K(ret), K(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dsl_query_info_->score_cols_.count(); i++) {
      if (OB_FAIL(add_dsl_expr(dsl_query_info_->score_cols_.at(i)))) {
        LOG_WARN("fail to add score column expr", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObDSLResolver::construct_dist_expr(ObColumnRefRawExpr *field_expr, ObRawExpr *vector_expr, ObVectorIndexDistAlgorithm dist_algo, ObRawExpr *&distance_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  ObSysFunRawExpr *dist_func_expr = nullptr;
  switch (dist_algo) {
    case ObVectorIndexDistAlgorithm::VIDA_IP: {
      // distance_expr: negative_inner_product(vec_field, query_vec)
      if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SYS_NEGATIVE_INNER_PRODUCT, dist_func_expr))) {
        LOG_WARN("fail to create negative inner product expr", K(ret));
      } else if (OB_FALSE_IT(dist_func_expr->set_func_name(N_VECTOR_NEGATIVE_INNER_PRODUCT))) {
      } else if (OB_FAIL(dist_func_expr->set_param_exprs(field_expr, vector_expr))) {
        LOG_WARN("fail to set param exprs for negative inner product", K(ret));
      } else {
        distance_expr = dist_func_expr;
      }
      break;
    }
    case ObVectorIndexDistAlgorithm::VIDA_L2: {
      // distance_expr: l2_distance(vec_field, query_vec)
      if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SYS_L2_DISTANCE, dist_func_expr))) {
        LOG_WARN("fail to create l2 distance expr", K(ret));
      } else if (OB_FALSE_IT(dist_func_expr->set_func_name(N_VECTOR_L2_DISTANCE))) {
      } else if (OB_FAIL(dist_func_expr->set_param_exprs(field_expr, vector_expr))) {
        LOG_WARN("fail to set param exprs for l2 distance", K(ret));
      } else {
        distance_expr = dist_func_expr;
      }
      break;
    }
    case ObVectorIndexDistAlgorithm::VIDA_COS: {
      // distance_expr: cosine_distance(vec_field, query_vec)
      if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SYS_COSINE_DISTANCE, dist_func_expr))) {
        LOG_WARN("fail to create cosine distance expr", K(ret));
      } else if (OB_FALSE_IT(dist_func_expr->set_func_name(N_VECTOR_COS_DISTANCE))) {
      } else if (OB_FAIL(dist_func_expr->set_param_exprs(field_expr, vector_expr))) {
        LOG_WARN("fail to set param exprs for cosine distance", K(ret));
      } else {
        distance_expr = dist_func_expr;
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported distance algorithm", K(ret), K(dist_algo));
      break;
    }
  }
  return ret;
}

int ObDSLResolver::construct_required_params(const char *param_names[], uint32_t name_count, RequiredParamsSet &required_params)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(required_params.create(name_count))) {
    LOG_WARN("failed to create params name set", K(ret));
  } else {
    for (uint32_t idx = 0; OB_SUCC(ret) && idx < name_count; ++idx) {
      ObString para_name(strlen(param_names[idx]), param_names[idx]);
      if (OB_FAIL(required_params.set_refactored(para_name))) {
        LOG_WARN("failed to set_refactored required params", K(ret));
      }
    }
  }
  return ret;
}

int ObDSLResolver::construct_score_columns()
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  char *buf = nullptr;
  int64_t pos = 0;
  uint64_t knn_idx = 0;
  ObOpPseudoColumnRawExpr *total_score = nullptr;
  ObRawExprResType res_type;
  res_type.set_double();
  for (int64_t i = 0; OB_SUCC(ret) && i < dsl_query_info_->queries_.count(); i++) {
    ObDSLQuery *query = dsl_query_info_->queries_.at(i);
    ObOpPseudoColumnRawExpr *sub_score = nullptr;
    char *buf2 = nullptr;
    int64_t pos2 = 0;
    if (OB_ISNULL(query)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query is null", K(ret), K(i));
    } else if (OB_ISNULL(buf2 = static_cast<char *>(allocator_->alloc(OB_MAX_COLUMN_NAME_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory hybrid search score name", K(ret));
    } else if (query->query_type_ == QUERY_ITEM_KNN &&
               OB_FAIL(databuff_printf(buf2, OB_MAX_COLUMN_NAME_LENGTH, pos2, "%.*s%lu",
                                       VS_SCORE_PREFIX.length(), VS_SCORE_PREFIX.ptr(), knn_idx++))) {
      LOG_WARN("fail to format vs score name", K(ret));
    } else if (query->query_type_ != QUERY_ITEM_KNN &&
               OB_FAIL(databuff_printf(buf2, OB_MAX_COLUMN_NAME_LENGTH, pos2, "%.*s",
                                       FTS_SCORE_NAME.length(), FTS_SCORE_NAME.ptr()))) {
      LOG_WARN("fail to format fts score name", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_op_pseudo_column_expr(*expr_factory,
                                                                    T_HYBRID_SEARCH_SCORE,
                                                                    buf2,
                                                                    res_type,
                                                                    sub_score))) {
      LOG_WARN("fail to build pseudo column expr", K(ret));
    } else if (OB_FAIL(dsl_query_info_->score_cols_.push_back(sub_score))) {
      LOG_WARN("fail to push back score expr", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(OB_MAX_COLUMN_NAME_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory hybrid search score name", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, OB_MAX_COLUMN_NAME_LENGTH, pos, "%s", OB_HYBRID_SEARCH_SCORE_COLUMN_NAME))) {
    LOG_WARN("fail to format hybrid search score name", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_op_pseudo_column_expr(*expr_factory,
                                                                 T_HYBRID_SEARCH_SCORE,
                                                                 buf,
                                                                 res_type,
                                                                 total_score))) {
    LOG_WARN("fail to build pseudo column expr", K(ret));
  } else if (OB_FAIL(dsl_query_info_->score_cols_.push_back(total_score))) {
    LOG_WARN("fail to push back score expr", K(ret));
  } else if (dsl_query_info_->score_cols_.count() != dsl_query_info_->queries_.count() + 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("score columns count is not equal to queries count + 1", K(ret), K(dsl_query_info_->score_cols_.count()), K(dsl_query_info_->queries_.count()));
  }
  return ret;
}

int ObDSLResolver::construct_rowkey_columns()
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> rowkey_col_ids;
  if (OB_FAIL(table_schema_->get_rowkey_column_ids(rowkey_col_ids))) {
    LOG_WARN("fail to get rowkey column ids", K(ret));
  } else {
    ObRawExprFactory *expr_factory = params_.expr_factory_;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_col_ids.count(); i++) {
      uint64_t col_id = rowkey_col_ids.at(i);
      ObColumnRefRawExpr *col_expr = nullptr;
      ObString col_name;
      bool is_column_exist = false;
      table_schema_->get_column_name_by_column_id(col_id, col_name, is_column_exist);
      if (!is_column_exist) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column not found by column id", K(ret), K(col_id));
      } else if (OB_FAIL(get_user_column_expr(col_name, col_expr))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          const ObColumnSchemaV2 *col_schema = table_schema_->get_column_schema(col_id);
          if (OB_ISNULL(col_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column schema is null", K(ret), K(col_id), K(col_name));
          } else if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *col_schema, session_info_, col_expr))) {
            LOG_WARN("fail to build column expr", K(ret), K(col_id), K(col_name));
          } else {
            setup_column_expr_attr(col_expr);
          }
        } else if (OB_NOT_SUPPORTED == ret && col_expr != nullptr) {
          if (col_expr->is_virtual_generated_column()) {
            ret = OB_SUCCESS;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column expr is not a virtual generated column", K(ret), K(col_name), K(col_id));
          }
        } else {
          LOG_WARN("fail to get user column expr", K(ret), K(col_name), K(col_id));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(col_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr is null", K(ret), K(col_id));
      } else if (OB_FAIL(dsl_query_info_->rowkey_cols_.push_back(col_expr))) {
        LOG_WARN("fail to push back rowkey column expr", K(ret), K(col_id));
      }
    }
  }
  return ret;
}

int ObDSLResolver::construct_string_expr(const ObString &str_value, ObRawExpr *&expr, ObCollationType collation_type/*=CS_TYPE_INVALID*/)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  ObCollationType collation_connection = collation_type;
  ObConstRawExpr *const_expr = nullptr;
  if (collation_connection == CS_TYPE_INVALID &&
      OB_FAIL(session_info_->get_collation_connection(collation_connection))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*expr_factory, ObVarcharType, str_value, collation_connection, const_expr))) {
    LOG_WARN("fail to create const string expr", K(ret));
  } else {
    expr = const_expr;
  }
  return ret;
}

int ObDSLResolver::append_wildcard_pattern_char(char *buf, const int64_t max_len, int64_t &pos, const char ch)
{
  int ret = OB_SUCCESS;
  if (pos >= max_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("wildcard pattern buffer overflow", K(ret), K(max_len), K(pos), K(ch));
  } else {
    buf[pos++] = ch;
  }
  return ret;
}

int ObDSLResolver::append_wildcard_like_escaped_char(char *buf, const int64_t max_len, int64_t &pos, const char ch)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append_wildcard_pattern_char(buf, max_len, pos, '\\'))) {
    LOG_WARN("failed to append wildcard escaped char", K(ret));
  } else if (OB_FAIL(append_wildcard_pattern_char(buf, max_len, pos, ch))) {
    LOG_WARN("failed to append wildcard pattern char", K(ret));
  }
  return ret;
}

int ObDSLResolver::convert_wildcard_pattern_to_like(const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  const int64_t max_len = src.length() * 2 + 1;
  char *buf = nullptr;
  int64_t pos = 0;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(max_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate wildcard conversion buffer", K(ret), K(src.length()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < src.length(); ++i) {
      const char ch = src.ptr()[i];
      if (ch == '\\') {
        if (i + 1 >= src.length()) {
          // Example: "abc\" -> "abc\\"
          if (OB_FAIL(append_wildcard_like_escaped_char(buf, max_len, pos, '\\'))) {
            LOG_WARN("failed to append wildcard escaped char", K(ret));
          }
        } else {
          const char next = src.ptr()[i + 1];
          switch (next) {
            case '*':
            case '?': {
              // Example: "\*" -> "*", "\?" -> "?"
              if (OB_FAIL(append_wildcard_pattern_char(buf, max_len, pos, next))) {
                LOG_WARN("failed to append wildcard pattern char", K(ret));
              } else {
                ++i;
              }
              break;
            }
            case '%':
            case '_':
            case '\\': {
              // Example: "\%" -> "\%", "\_" -> "\_", "\\" -> "\\"
              if (OB_FAIL(append_wildcard_like_escaped_char(buf, max_len, pos, next))) {
                LOG_WARN("failed to append wildcard escaped char", K(ret));
              } else {
                ++i;
              }
              break;
            }
            default: {
              // Example: "\a" -> "\\a"
              if (OB_FAIL(append_wildcard_like_escaped_char(buf, max_len, pos, '\\'))) {
                LOG_WARN("failed to append wildcard escaped char", K(ret));
              }
              break;
            }
          }
        }
      } else if (ch == '*') {
        // Example: "ab*cd" -> "ab%cd"
        if (OB_FAIL(append_wildcard_pattern_char(buf, max_len, pos, '%'))) {
          LOG_WARN("failed to append wildcard pattern char", K(ret));
        }
      } else if (ch == '?') {
        // Example: "ab?cd" -> "ab_cd"
        if (OB_FAIL(append_wildcard_pattern_char(buf, max_len, pos, '_'))) {
          LOG_WARN("failed to append wildcard pattern char", K(ret));
        }
      } else if (ch == '%' || ch == '_') {
        // Example: "a%b" -> "a\%b", "a_b" -> "a\_b"
        if (OB_FAIL(append_wildcard_like_escaped_char(buf, max_len, pos, ch))) {
          LOG_WARN("failed to append wildcard escaped char", K(ret));
        }
      } else {
        // Example: "abc" -> "abc"
        if (OB_FAIL(append_wildcard_pattern_char(buf, max_len, pos, ch))) {
          LOG_WARN("failed to append wildcard pattern char", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    dst.assign_ptr(buf, pos);
  }
  return ret;
}

int ObDSLResolver::resolve_query_string_expr(const ObString &str_value, const ObCollationType target_coll, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObCollationType session_coll = CS_TYPE_INVALID;
  ObString query_str_value;
  if (OB_FAIL(session_info_->get_collation_connection(session_coll))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (session_coll == target_coll) {
    // skip convert
    query_str_value = str_value;
  } else if (OB_FAIL(ObCharset::charset_convert(*allocator_, str_value, session_coll, target_coll, query_str_value))) {
    LOG_WARN("fail to convert string", K(ret), K(session_coll), K(target_coll));
    if (OB_ERR_INCORRECT_STRING_VALUE == ret) {
      ret = OB_ERR_INVALID_CHARACTER_STRING;
      const char *tmp_charset_name = ObCharset::charset_name(target_coll);
      int64_t tmp_charset_name_len = strlen(tmp_charset_name);
      const int64_t buf_len = tmp_charset_name_len + 128;
      char tmp_buf[buf_len];
      memset(tmp_buf, 0, buf_len);
      int64_t tmp_buf_len = snprintf(tmp_buf, sizeof(tmp_buf),
          "query text with %s", tmp_charset_name);
      if (tmp_buf_len < 0) {
        tmp_buf_len = 0;
        LOG_WARN("snprintf failed");
      } else if (tmp_buf_len >= buf_len) {
        tmp_buf_len = buf_len - 1;
        LOG_WARN("snprintf buffer overflow, string truncated");
      }
      LOG_USER_ERROR(OB_ERR_INVALID_CHARACTER_STRING,
          static_cast<int>(tmp_buf_len), tmp_buf,
          str_value.length(), str_value.ptr());
    }
  }

  if (FAILEDx(construct_string_expr(query_str_value, expr, target_coll))) {
    LOG_WARN("fail to construct string expr", K(ret));
  }
  return ret;
}

int ObDSLResolver::set_const_long_text_prefix_len(ObRawExpr *src_expr, ObIArray<ObRawExpr*> &longtext_exprs, ObIArray<int32_t> &origin_lens)
{
  int ret = OB_SUCCESS;
  const int32_t PRE_FIX_LEN = 200;
  if (ObRawExpr::EXPR_CONST == src_expr->get_expr_class() && static_cast<ObConstRawExpr *>(src_expr)->get_value().is_varchar() &&
      static_cast<ObConstRawExpr *>(src_expr)->get_value().get_string_len() >= PRE_FIX_LEN) {
    if (OB_FAIL(longtext_exprs.push_back(src_expr))) {
      LOG_WARN("failed to push back param", K(ret));
    } else if (OB_FAIL(origin_lens.push_back(static_cast<ObConstRawExpr *>(src_expr)->get_value().get_string_len()))) {
      LOG_WARN("failed to push back param", K(ret));
    } else {
      static_cast<ObConstRawExpr *>(src_expr)->get_value().val_len_ = PRE_FIX_LEN;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < src_expr->get_param_count(); ++i) {
      ObRawExpr *param_expr = src_expr->get_param_expr(i);
      if (OB_ISNULL(param_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param src_expr is null", K(ret));
      } else if (OB_FAIL(SMART_CALL(set_const_long_text_prefix_len(src_expr->get_param_expr(i), longtext_exprs, origin_lens)))) {
        LOG_WARN("failed to extract const params", K(ret));
      }
    }
  }
  return ret;
}

int ObDSLResolver::formalize_exprs()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < dsl_query_info_->dsl_exprs_.count(); i++) {
    ObRawExpr *expr = dsl_query_info_->dsl_exprs_.at(i);
    ObSEArray<ObRawExpr*, 1> longtext_exprs;
    ObSEArray<int32_t, 1> origin_lens;
    const int32_t PRE_FIX_LEN = 200;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null expr", K(ret), K(i));
    } else if (OB_FAIL(set_const_long_text_prefix_len(expr, longtext_exprs, origin_lens))) {
      LOG_WARN("failed to set prefix longtext len", K(ret), K(i));
    } else if (OB_FAIL(expr->formalize(session_info_))) {
      LOG_WARN("fail to formalize expr", K(ret), K(i), KPC(expr));
    } else {
      for (int j = 0; j < longtext_exprs.count(); j++) {
        static_cast<ObConstRawExpr *>(longtext_exprs.at(j))->get_value().val_len_ = origin_lens.at(j);
      }
    }
  }
  return ret;
}

int ObDSLResolver::get_col_idx_info(const ObString &col_name, ObColumnIndexInfo *&idx_info)
{
  int ret = OB_SUCCESS;
  idx_info = nullptr;
  if (!col_idx_map_.created()) {
    // do nothing
  } else if (OB_FAIL(col_idx_map_.get_refactored(col_name, idx_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get col idx info", K(ret), K(col_name));
    }
  }
  return ret;
}

int ObDSLResolver::get_dist_algo_type(ObColumnRefRawExpr *field_expr, ObVectorIndexDistAlgorithm &algo_type)
{
  int ret = OB_SUCCESS;
  ObColumnIndexInfo *idx_info = nullptr;
  if (OB_FAIL(get_col_idx_info(field_expr->get_column_name(), idx_info))) {
    LOG_WARN("fail to get col idx info", K(ret), K(field_expr->get_column_name()));
  } else if (OB_ISNULL(idx_info)) {
    // do nothing
  } else if (!is_local_vec_hnsw_index(idx_info->index_type_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "knn search for vector column for non-hnsw index");
    LOG_WARN("vector index is not a hnsw index", K(ret), K(idx_info->index_type_));
  } else {
    algo_type = idx_info->dist_algorithm_;
  }
  return ret;
}

int ObDSLResolver::get_field_expr_and_path(const ObString &field_name, ObColumnRefRawExpr *&col_expr, ObString &path_str)
{
  int ret = OB_SUCCESS;
  const char *dot_pos = nullptr;
  ObString col_name;
  ObString raw_path_str;
  if (OB_ISNULL(dot_pos = field_name.find('.'))) {
    col_name = field_name;
  } else if (dot_pos == field_name.ptr() || dot_pos == field_name.ptr() + field_name.length() - 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "field, invalid field with path");
    LOG_WARN("invalid field with path", K(ret), K(field_name));
  } else {
    col_name = ObString(dot_pos - field_name.ptr(), field_name.ptr());
    raw_path_str = ObString(field_name.length() - col_name.length() - 1, dot_pos + 1);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_user_column_expr(col_name, col_expr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "field, column not exists");
    }
    LOG_WARN("fail to get user column expr", K(ret), K(col_name));
  } else if (OB_ISNULL(dot_pos)) {
    // no path, do nothing
  } else if (!col_expr->get_result_type().is_json()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "json_extract, column is not a json type");
    LOG_WARN("json_extract for non-json column", K(ret), K(col_name));
  } else {
    char *json_path_buf = nullptr;
    int64_t json_path_len = raw_path_str.length() + 2;
    if (OB_ISNULL(json_path_buf = static_cast<char *>(allocator_->alloc(json_path_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for json path", K(ret));
    } else {
      json_path_buf[0] = '$';
      json_path_buf[1] = '.';
      MEMCPY(json_path_buf + 2, raw_path_str.ptr(), raw_path_str.length());
      path_str.assign_ptr(json_path_buf, json_path_len);
    }
  }
  return ret;
}

int ObDSLResolver::get_fulltext_index_schema(const ObString &col_name, const ObTableSchema *&index_schema)
{
  int ret = OB_SUCCESS;
  ObColumnIndexInfo *idx_info = nullptr;
  if (OB_FAIL(get_col_idx_info(col_name, idx_info))) {
    LOG_WARN("fail to get col idx info", K(ret), K(col_name));
  } else if (OB_ISNULL(idx_info)) {
    // do nothing
  } else if (OB_ISNULL(idx_info->index_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schema is null", K(ret), K(col_name));
  } else if (!idx_info->index_schema_->is_fts_index_aux()) {
  } else {
    index_schema = idx_info->index_schema_;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_FT_COLUMN_NOT_INDEXED;
    LOG_WARN("no matched fulltext index found", K(col_name), K(ret));
  }
  return ret;
}

int ObDSLResolver::get_json_string_from_node(const ParseNode *node, ObString &json_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node is null", K(ret));
  } else if (T_USER_VARIABLE_IDENTIFIER == node->type_) {
    ObObj var_value;
    ObString var_name;
    var_name.assign_ptr(node->str_value_, static_cast<int32_t>(node->str_len_));
    if (OB_FAIL(session_info_->get_user_variable_value(var_name, var_value))) {
      LOG_WARN("failed to get user variable value", K(ret), K(var_name));
    } else if (var_value.is_null()) {
      ret = OB_ERR_USER_VARIABLE_UNKNOWN;
      LOG_WARN("user variable is null", K(ret), K(var_name));
    } else if (OB_FAIL(var_value.get_string(json_str))) {
      LOG_WARN("failed to get string from user variable", K(ret), K(var_name));
    } else if (!session_info_->is_remote_session()) {
      // register user variable to query_ctx->variables_ locally for remote execution serialization
      ObQueryCtx *query_ctx = stmt_->get_query_ctx();
      ObVarInfo var_info;
      var_info.name_ = var_name;
      var_info.type_ = USER_VAR;
      if (OB_ISNULL(query_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query ctx is null", K(ret));
      } else if (OB_FAIL(query_ctx->variables_.push_back(var_info))) {
        LOG_WARN("failed to register user variable for serialization", K(ret), K(var_name));
      }
    }
  } else {
    json_str.assign_ptr(node->str_value_, node->str_len_);
  }
  return ret;
}

int ObDSLResolver::get_user_column_expr(ObString &col_name, ObColumnRefRawExpr *&col_expr)
{
  int ret = OB_SUCCESS;
  bool exists = false;
  if (col_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column name is empty", K(ret));
  } else if (OB_FAIL(ObCharset::tolower(CS_TYPE_UTF8MB4_GENERAL_CI, col_name, col_name, *allocator_))) {
    LOG_WARN("fail to lower column name", K(ret), K(col_name));
  } else if (OB_FAIL(has_user_column_name(col_name, exists, &col_expr))) {
    LOG_WARN("fail to lookup user column", K(ret), K(col_name));
  } else if (!exists) {
    // OB_HASH_NOT_EXIST would not be logged here and should be handled in the caller
    ret = OB_HASH_NOT_EXIST;
  }
  return ret;
}

int ObDSLResolver::init_bool_info(ObIJsonBase &req_node, ObConstRawExpr *&msm_expr, ObConstRawExpr *&boost_expr)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *msm_node = nullptr;
  bool has_must = false;
  bool has_filter = false;
  bool has_should = false;
  msm_expr = nullptr;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "bool query, should be object");
    LOG_WARN("bool query should be object", K(ret), K(req_node.json_type()));
  } else {
    for (uint64_t i = 0; OB_SUCC(ret) && i < req_node.element_count(); i++) {
      ObString key;
      ObIJsonBase *sub_node = nullptr;
      if (OB_FAIL(req_node.get_object_value(i, key, sub_node))) {
        LOG_WARN("fail to get value", K(ret), K(i));
      } else if (key.case_compare("must") == 0) {
        has_must = true;
      } else if (key.case_compare("filter") == 0) {
        has_filter = true;
      } else if (key.case_compare("should") == 0) {
        has_should = true;
      } else if (key.case_compare("must_not") == 0) {
        // do nothing
      } else if (key.case_compare("minimum_should_match") == 0) {
        msm_node = sub_node;
      } else if (key.case_compare("boost") == 0) {
        if (OB_FAIL(resolve_boost(*sub_node, boost_expr, QUERY_ITEM_BOOL, QUERY_ITEM_UNKNOWN))) {
          LOG_WARN("fail to resolve boost", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "keys other than \"must\", \"should\", \"filter\", \"must_not\", \"minimum_should_match\" and \"boost\" in bool query");
        LOG_WARN("unsupported key in bool query", K(ret), K(key));
      }
    }
    if (OB_SUCC(ret)) {
      int32_t default_bool_msm = 0;
      if (OB_ISNULL(msm_node)) {
        default_bool_msm = static_cast<int32_t>((has_should && !has_must && !has_filter) ? 1 : 0);
      }
      if (OB_FAIL(resolve_minimum_should_match_expr(msm_node, CS_TYPE_INVALID, default_bool_msm, msm_expr))) {
        LOG_WARN("fail to build default bool minimum_should_match expr", KR(ret));
      }
    }
  }
  return ret;
}

int ObDSLResolver::init_col_idx_map()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 4, ModulePageAllocator, true> simple_index_infos;
  if (col_idx_map_.created()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("col idx map already initialized, should not call init again", K(ret));
  } else if (OB_FAIL(table_schema_->get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("fail to get simple index infos", K(ret));
  } else if (simple_index_infos.empty()) {
    // do nothing
  } else if (OB_FAIL(col_idx_map_.create(simple_index_infos.count(), "HybridSearch"))) {
    LOG_WARN("fail to create col idx map", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); i++) {
      const ObTableSchema *index_schema = nullptr;
      if (OB_FAIL(schema_checker_->get_table_schema(table_schema_->get_tenant_id(), simple_index_infos.at(i).table_id_, index_schema))) {
        LOG_WARN("fail to get index schema", K(ret), K(simple_index_infos.at(i).table_id_));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index table schema should not be null", K(ret), K(simple_index_infos.at(i).table_id_));
      } else if (index_schema->is_built_in_index()) {
        continue;
      } else {
        const ObRowkeyInfo &rowkey_info = index_schema->get_rowkey_info();
        for (int64_t j = 0; OB_SUCC(ret) && j < rowkey_info.get_size(); j++) {
          const ObRowkeyColumn *rowkey_column = nullptr;
          const ObColumnSchemaV2 *col_schema = nullptr;
          ObSEArray<uint64_t, 4, ModulePageAllocator, true> cascaded_column_ids;
          const ObColumnSchemaV2 *table_column = nullptr;
          if (OB_ISNULL(rowkey_column = rowkey_info.get_column(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("rowkey column is null", K(ret), K(j), KPC(index_schema));
          } else if (OB_ISNULL(col_schema = index_schema->get_column_schema(rowkey_column->column_id_))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(rowkey_column->column_id_), KPC(index_schema));
          } else if ((index_schema->is_fts_index() && !col_schema->is_fulltext_column()) ||
                     (index_schema->is_vec_index() && col_schema->is_vec_hnsw_vid_column()) ||
                     (!index_schema->is_fts_index() && !index_schema->is_vec_index())) {
            // do nothing
          } else if (OB_ISNULL(table_column = table_schema_->get_column_schema(col_schema->get_column_id()))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected table column", K(ret));
          } else if (OB_FAIL(table_column->get_cascaded_column_ids(cascaded_column_ids))) {
            LOG_WARN("failed to get cascaded_column_ids", K(ret));
          } else {
            ObSEArray<uint64_t, 4, ModulePageAllocator, true> user_column_ids;
            bool is_multi_column_index = false;
            for (int64_t k = 0; OB_SUCC(ret) && k < cascaded_column_ids.count(); k++) {
              const ObColumnSchemaV2 *cascaded_column = table_schema_->get_column_schema(cascaded_column_ids.at(k));
              if (cascaded_column->is_hidden() ||
                  cascaded_column->is_unused() ||
                  cascaded_column->is_invisible_column() ||
                  cascaded_column->is_shadow_column()) {
                // ignore these columns
              } else if (OB_FAIL(user_column_ids.push_back(cascaded_column->get_column_id()))) {
                LOG_WARN("fail to push back visible column id", K(ret), K(cascaded_column->get_column_id()));
              }
            }
            if (OB_SUCC(ret) && index_schema->is_fts_index_aux() && user_column_ids.count() > 1) {
              is_multi_column_index = true;
            }
            for (int64_t k = 0; OB_SUCC(ret) && !is_multi_column_index && k < user_column_ids.count(); k++) {
              const ObColumnSchemaV2 *cascaded_column = nullptr;
              ObString column_name;
              ObColumnIndexInfo *existing_idx_info = nullptr;
              if (OB_ISNULL(cascaded_column = table_schema_->get_column_schema(user_column_ids.at(k)))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected cascaded column", K(ret));
              } else if (OB_FALSE_IT(column_name = cascaded_column->get_column_name_str())) {
              } else if (OB_FAIL(ObCharset::tolower(CS_TYPE_UTF8MB4_GENERAL_CI, column_name, column_name, *allocator_))) {
                LOG_WARN("fail to lower column name", K(ret), K(column_name));
              } else if (OB_FAIL(col_idx_map_.get_refactored(column_name, existing_idx_info))) {
                if (ret == OB_HASH_NOT_EXIST) {
                  ret = OB_SUCCESS;
                  ObString index_name;
                  ObColumnIndexInfo *idx_info = nullptr;
                  if (OB_ISNULL(idx_info = OB_NEWx(ObColumnIndexInfo, allocator_))) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    LOG_WARN("fail to allocate memory for index info", K(ret));
                  } else if (OB_FAIL(ObTableSchema::get_index_name(*allocator_, table_schema_->get_table_id(),
                                                                   ObString::make_string(index_schema->get_table_name()), index_name))) {
                    LOG_WARN("get index name failed", K(ret));
                  } else if (FALSE_IT(idx_info->index_name_ = index_name)) {
                  } else if (FALSE_IT(idx_info->index_type_ = index_schema->get_index_type())) {
                  } else if (FALSE_IT(idx_info->index_schema_ = index_schema)) {
                  } else if (index_schema->is_vec_index()) {
                    ObVectorIndexType index_type = ObVectorIndexType::VIT_MAX;
                    ObVectorIndexParam index_param;
                    if (index_schema->is_vec_ivf_index()) {
                      index_type = ObVectorIndexType::VIT_IVF_INDEX;
                    } else if (index_schema->is_vec_hnsw_index()) {
                      index_type = ObVectorIndexType::VIT_HNSW_INDEX;
                    } else {
                      ret = OB_NOT_SUPPORTED;
                      LOG_WARN("unsupported vector index type", K(ret), K(index_schema->get_index_type()), K(index_schema->get_table_name()));
                    }
                    if (OB_FAIL(ret)) {
                    } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(index_schema->get_index_params(), index_type, index_param))) {
                      LOG_WARN("failed to parser vec index param", K(ret), K(index_schema->get_index_params()));
                    } else {
                      idx_info->dist_algorithm_ = index_param.dist_algorithm_;
                    }
                  }
                  if (OB_FAIL(ret)) {
                  } else if (OB_FAIL(col_idx_map_.set_refactored(column_name, idx_info))) {
                    LOG_WARN("fail to set refactored col idx map", K(ret), K(column_name));
                  }
                } else {
                  LOG_WARN("fail to get col idx info", K(ret), K(column_name));
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDSLResolver::init_col_schema_map()
{
  int ret = OB_SUCCESS;
  if (col_schema_map_.created()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user column map already initialized, should not call init again", K(ret));
  } else if (OB_FAIL(col_schema_map_.create(table_schema_->get_column_count(), "HybridSearch"))) {
    LOG_WARN("fail to create user column map", K(ret));
  } else {
    ObColumnIterByPrevNextID iter(*table_schema_);
    const ObColumnSchemaV2 *column_schema = nullptr;
    while (OB_SUCC(ret) && OB_SUCC(iter.next(column_schema))) {
      ObString col_name;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is NULL", K(ret));
      } else if (column_schema->is_hidden() ||
                 column_schema->is_unused() ||
                 column_schema->is_invisible_column() ||
                 column_schema->is_shadow_column()) {
        continue;
      } else if (OB_FALSE_IT(col_name = column_schema->get_column_name_str())) {
      } else if (OB_FAIL(ObCharset::tolower(CS_TYPE_UTF8MB4_GENERAL_CI, col_name, col_name, *allocator_))) {
        LOG_WARN("fail to lower column name", K(ret), K(col_name));
      } else if (OB_FAIL(col_schema_map_.set_refactored(col_name, column_schema))) {
        LOG_WARN("fail to set column schema in hash map", K(ret), K(col_name));
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObDSLResolver::init_resolver()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator_ is null", K(ret));
  } else if (OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_checker_ is null", K(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ is null", K(ret));
  } else if (OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr factory is null", K(ret));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema_ is null", K(ret));
  } else if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is null", K(ret));
  } else if (OB_INVALID_ID == table_item_.table_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_id_ is invalid", K(ret));
  } else if (OB_NOT_NULL(dsl_query_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dsl_query_info_ already initialized", K(ret));
  } else if (OB_ISNULL(dsl_query_info_ = OB_NEWx(ObDSLQueryInfo, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create dsl query info", K(ret));
  } else if (OB_FAIL(dsl_query_info_->init_default_params(*params_.expr_factory_))) {
    LOG_WARN("fail to init default params", K(ret));
  } else if (OB_FAIL(init_col_schema_map())) {
    LOG_WARN("fail to init user column map", K(ret));
  } else if (OB_FAIL(init_col_idx_map())) {
    LOG_WARN("fail to init col idx map", K(ret));
  }
  return ret;
}

int ObDSLResolver::is_array_column(ObColumnRefRawExpr *col_expr, bool &is_array_col, ObObjType &array_base_type)
{
  int ret = OB_SUCCESS;
  const ObSqlCollectionInfo *coll_info = nullptr;
  is_array_col = false;
  array_base_type = ObMaxType;
  if (!col_expr->get_result_type().is_collection_sql_type()) {
  } else if (OB_FAIL(ObRawExprUtils::get_expr_collection_info(col_expr, session_info_->get_cur_exec_ctx(), coll_info))) {
    LOG_WARN("fail to get collection meta for column", K(ret), KPC(col_expr));
  } else if (OB_ISNULL(coll_info) || OB_ISNULL(coll_info->collection_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("collection info or meta is null", K(ret), KPC(col_expr));
  } else if (coll_info->collection_meta_->type_id_ == ObNestedType::OB_ARRAY_TYPE) {
    uint32_t nest_depth = 0;
    is_array_col = true;
    array_base_type = coll_info->get_basic_meta(nest_depth).get_obj_type();
  }
  return ret;
}

// a lightweight wrapper to convert JSON node to string
int ObDSLResolver::print_json_node(ObIJsonBase &node, ObJsonBuffer &j_buf)
{
  int ret = OB_SUCCESS;
  bool is_quoted = false;
  const ObJsonNodeType json_type = node.json_type();
  if (json_type == ObJsonNodeType::J_STRING) {
    is_quoted = true;
  } else if (ObIJsonBase::is_json_number_type(json_type) ||
             json_type == ObJsonNodeType::J_BOOLEAN ||
             json_type == ObJsonNodeType::J_OBJECT) {
    is_quoted = false;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported json node to print string", K(ret), K(json_type));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(node.print(j_buf, is_quoted))) {
    LOG_WARN("fail to print json node to string", K(ret), K(json_type));
  }
  return ret;
}

int ObDSLResolver::resolve_array_contains(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type)
{
  return resolve_array_expr(req_node, query, parent_query, outer_query_type, QUERY_ITEM_ARRAY_CONTAINS);
}

int ObDSLResolver::resolve_array_contains_all(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type)
{
  return resolve_array_expr(req_node, query, parent_query, outer_query_type, QUERY_ITEM_ARRAY_CONTAINS_ALL);
}

int ObDSLResolver::resolve_array_expr(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type, ObEsQueryItem query_type)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  ObItemType expr_type = T_MAX;
  ObString col_name;
  ObString expr_name;
  ObJsonNodeType arg_type = ObJsonNodeType::J_MAX_TYPE;
  bool is_array_col = false;
  ObObjType array_base_type = ObMaxType;
  ObIJsonBase *param_node = nullptr;
  ObIJsonBase *arg_node = nullptr;
  ObColumnRefRawExpr *col_expr = nullptr;
  ObRawExpr *arg_expr = nullptr;
  ObSysFunRawExpr *array_expr = nullptr;
  ObConstRawExpr *boost_expr = nullptr;
  ObDSLScalarQuery *array_query = nullptr;
  if (query_type == QUERY_ITEM_ARRAY_CONTAINS) {
    expr_type = T_FUNC_SYS_ARRAY_CONTAINS;
    expr_name = N_ARRAY_CONTAINS;
  } else if (query_type == QUERY_ITEM_ARRAY_CONTAINS_ALL) {
    expr_type = T_FUNC_SYS_ARRAY_CONTAINS_ALL;
    expr_name = N_ARRAY_CONTAINS_ALL;
  } else if (query_type == QUERY_ITEM_ARRAY_OVERLAPS) {
    expr_type = T_FUNC_SYS_ARRAY_OVERLAPS;
    expr_name = N_ARRAY_OVERLAPS;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported array query type", K(ret), K(query_type));
  }
  if (OB_FAIL(ret)) {
  } else if (ObDSLQuery::check_need_cal_score_in_bool(outer_query_type, parent_query)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "array query in must/should clause");
    LOG_WARN("array query cannot be scored or exist in must/should clause", K(ret), K(query_type));
  } else if (req_node.json_type() != ObJsonNodeType::J_OBJECT || req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "array query, should be single-key object");
    LOG_WARN("array query should be single-key object", K(ret));
  } else if (OB_FAIL(req_node.get_object_value(0, col_name, param_node))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (param_node->json_type() == ObJsonNodeType::J_OBJECT) {
    for (uint64_t i = 0; OB_SUCC(ret) && i < param_node->element_count(); i++) {
      ObString key;
      ObIJsonBase *sub_node = nullptr;
      if (OB_FAIL(param_node->get_object_value(i, key, sub_node))) {
        LOG_WARN("fail to get value", K(ret));
      } else if (key.case_compare("arg") == 0) {
        arg_node = sub_node;
      } else if (key.case_compare("boost") == 0) {
        if (OB_FAIL(resolve_boost(*sub_node, boost_expr, query_type, outer_query_type))) {
          LOG_WARN("fail to resolve boost", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "keys other than \"arg\" and \"boost\" in array query");
        LOG_WARN("unsupported key in array query", K(ret), K(key), K(query_type));
      }
    }
  } else {
    arg_node = param_node;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(arg_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "array query, arg not exists");
    LOG_WARN("arg not exists in array query", K(ret));
  } else if (OB_FAIL(get_user_column_expr(col_name, col_expr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "array expression, column not exists");
    }
    LOG_WARN("fail to get user column expr", K(ret), K(col_name));
  } else if (OB_FAIL(is_array_column(col_expr, is_array_col, array_base_type))) {
    LOG_WARN("fail to check if column is array", K(ret), KPC(col_expr));
  } else if (!is_array_col) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "array expression, column is not an array type");
    LOG_WARN("array expression for non-array column", K(ret), K(col_name));
  } else if (OB_FALSE_IT(arg_type = resolve_type_mapping(arg_node->json_type(), col_expr, query_type, array_base_type))) {
  } else if (OB_FAIL(resolve_const(*arg_node, arg_expr, arg_type, query_type, array_base_type))) {
    LOG_WARN("fail to resolve arg expr", K(ret));
  } else if (OB_FAIL(expr_factory->create_raw_expr(expr_type, array_expr))) {
    LOG_WARN("fail to create array expr", K(ret));
  } else if (OB_FALSE_IT(array_expr->set_func_name(expr_name))) {
  } else if (OB_FAIL(array_expr->set_param_exprs(col_expr, arg_expr))) {
    LOG_WARN("fail to set param exprs for array expr", K(ret));
  } else if (OB_FAIL(ObDSLScalarQuery::create(*allocator_, array_query, query_type, outer_query_type, parent_query))) {
    LOG_WARN("fail to create array query", K(ret));
  } else {
    array_query->field_ = col_expr;
    array_query->scalar_expr_ = array_expr;
    array_query->boost_ = setup_boost(boost_expr);
    query = array_query;
  }
  return ret;
}

int ObDSLResolver::resolve_array_overlaps(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type)
{
  return resolve_array_expr(req_node, query, parent_query, outer_query_type, QUERY_ITEM_ARRAY_OVERLAPS);
}

int ObDSLResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObJsonNode *j_node = nullptr;
  ObIJsonBase *dsl_sort_node = nullptr;
  const char *syntaxerr = nullptr;
  uint64_t err_offset = 0;
  ObString req_str;
  ParseNode *raw_text = parse_tree.children_[0];
  uint32_t parse_flag = ObJsonParser::JSN_UNIQUE_FLAG;
  if (OB_FAIL(init_resolver())) {
    LOG_WARN("fail to init resolver", K(ret));
  } else if (OB_FAIL(get_json_string_from_node(raw_text, req_str))) {
    LOG_WARN("failed to get json string from node", K(ret));
  } else if (OB_FAIL(ObJsonParser::parse_json_text(allocator_, req_str.ptr(), req_str.length(), syntaxerr, &err_offset, j_node, parse_flag))) {
    LOG_WARN("failed to parse array text", K(ret), K(req_str), KCSTRING(syntaxerr), K(err_offset));
  } else if (j_node->json_type() != ObJsonNodeType::J_OBJECT || j_node->element_count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid search param, empty dsl");
    LOG_WARN("invalid empty dsl", K(ret), K(j_node->json_type()), K(j_node->element_count()));
  } else if (OB_FAIL(resolve_default_params(*j_node))) {
    LOG_WARN("fail to resolve default params", K(ret));
  } else {
    dsl_query_info_->raw_dsl_param_str_ = req_str;
    uint64_t count = j_node->element_count();
    for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      ObString key;
      ObIJsonBase *req_node = nullptr;
      if (OB_FAIL(j_node->get_object_value(i, key, req_node))) {
        LOG_WARN("fail to get value", K(ret), K(i));
      } else if (key.case_compare("query") == 0) {
        if (OB_FAIL(resolve_query(*req_node))) {
          LOG_WARN("fail to get value", K(ret), K(i));
        }
      } else if (key.case_compare("knn") == 0) {
        if (OB_FAIL(resolve_multi_knn(*req_node))) {
          LOG_WARN("fail to resolve multi knn", K(ret), K(i));
        }
      } else if (key.case_compare("from") == 0) {
        // do nothing
      } else if (key.case_compare("size") == 0) {
        // do nothing
      } else if (key.case_compare("rank") == 0) {
        // do nothing
      } else if (key.case_compare("min_score") == 0) {
        // do nothing
      } else if (key.case_compare("rerank") == 0) {
        // do nothing
      } else if (key.case_compare("sort") == 0) {
        dsl_sort_node = req_node;
      } else if (key.case_compare("collapse") == 0) {
        if (OB_FAIL(resolve_collapse(*req_node))) {
          LOG_WARN("fail to resolve collapse", K(ret), K(i));
        }
      } else if (key.case_compare("aggs") == 0) {
        if (OB_NOT_NULL(dsl_sort_node)) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "aggs with sort");
          LOG_WARN("aggs and sort cannot be used together", K(ret));
        } else if (OB_FAIL(resolve_aggs(*req_node))) {
          LOG_WARN("fail to resolve aggs", K(ret), K(i));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "key in search param");
        LOG_WARN("unsupported key in search param", K(ret), K(key));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (dsl_query_info_->queries_.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid search param, must contain at least one query");
      LOG_WARN("invalid search param, must contain at least one query", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_result_mode_and_track_score())) {
    LOG_WARN("failed to init result mode and track score", K(ret));
  } else if (OB_FAIL(construct_rowkey_columns())) {
    LOG_WARN("fail to construct rowkey columns", K(ret));
  } else if (OB_FAIL(construct_score_columns())) {
    LOG_WARN("fail to construct score columns", K(ret));
  } else if (OB_NOT_NULL(dsl_sort_node) && OB_FAIL(resolve_sort(*dsl_sort_node))) {
    LOG_WARN("fail to resolve dsl sort", K(ret));
  } else if (dsl_query_info_->has_dsl_collapse() && OB_FAIL(inject_collapse_stmt_rewrites())) {
    LOG_WARN("fail to inject collapse stmt rewrites", K(ret));
  } else if (dsl_query_info_->has_dsl_aggs() && OB_FAIL(inject_agg_stmt_rewrites())) {
    LOG_WARN("fail to inject agg stmt rewrites", K(ret));
  } else if (OB_FAIL(collect_exprs())) {
    LOG_WARN("fail to collect exprs", K(ret));
  } else if (OB_FAIL(formalize_exprs())) {
    LOG_WARN("fail to formalize exprs", K(ret));
  }
  return ret;
}

 int ObDSLResolver::set_stmt_limit_offset(ObDSLQueryInfo *dsl_query_info, ObSelectStmt &select_stmt)
{
  int ret = OB_SUCCESS;
  ObRawExpr *offset_expr = nullptr;
  if (OB_ISNULL(dsl_query_info) || OB_ISNULL(dsl_query_info->size_) || OB_ISNULL(dsl_query_info->from_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dsl query info or size expr or from expr is null", K(ret), KP(dsl_query_info));
  } else {
    int64_t from_value = 0;
    const ObConstRawExpr *from_expr = static_cast<const ObConstRawExpr *>(dsl_query_info->from_);
    if (OB_FAIL(from_expr->get_value().get_int(from_value))) {
      LOG_WARN("failed to get from value", K(ret));
    } else if (from_value > 0) {
      offset_expr = dsl_query_info->from_;
    }
  }
  if (OB_SUCC(ret)) {
    select_stmt.set_limit_offset(dsl_query_info->size_, offset_expr);
  }
  return ret;
}


int ObDSLResolver::resolve_sort(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  if (dsl_query_info_->has_dsl_aggs()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "aggs with sort");
    LOG_WARN("sort is not supported when aggs is specified", K(ret));
  } else if (dsl_query_info_->has_dsl_rank()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "sort with rank");
    LOG_WARN("sort is not supported when rank is specified", K(ret));
  } else if (req_node.json_type() != ObJsonNodeType::J_ARRAY || req_node.element_count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sort, should not be empty array");
    LOG_WARN("sort should not be empty array", K(ret));
  } else {
    ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(stmt_);
    const uint64_t sort_count = req_node.element_count();
    for (uint64_t i = 0; OB_SUCC(ret) && i < sort_count; ++i) {
      ObIJsonBase *sort_item = nullptr;
      if (OB_FAIL(req_node.get_array_element(i, sort_item))) {
        LOG_WARN("fail to get sort array element", K(ret), K(i));
      } else if (OB_FAIL(resolve_sort_item(*sort_item, *select_stmt))) {
        LOG_WARN("failed to resolve sort item", K(ret), K(i));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(set_stmt_limit_offset(dsl_query_info_, *select_stmt))) {
      LOG_WARN("failed to set stmt limit for hybrid sort", K(ret));
    }
  }
  return ret;
}

int ObDSLResolver::resolve_sort_item(ObIJsonBase &sort_item, ObSelectStmt &select_stmt)
{
  int ret = OB_SUCCESS;
  if (sort_item.json_type() == ObJsonNodeType::J_STRING) {
    ObString field_name(sort_item.get_data_length(), sort_item.get_data());
    if (OB_FAIL(resolve_sort_string_item(field_name, select_stmt))) {
      LOG_WARN("failed to resolve shorthand sort item", K(ret), K(field_name));
    }
  } else if (sort_item.json_type() == ObJsonNodeType::J_OBJECT) {
    if (OB_FAIL(resolve_sort_object_item(sort_item, select_stmt))) {
      LOG_WARN("failed to resolve object sort item", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sort item, should be string or object");
    LOG_WARN("invalid sort item type", K(ret));
  }
  return ret;
}

int ObDSLResolver::resolve_sort_string_item(const ObString &field_name, ObSelectStmt &select_stmt)
{
  int ret = OB_SUCCESS;
  ObDSLSortItem dsl_sort_item;
  if (OB_FAIL(resolve_sort_field(field_name, false, dsl_sort_item))) {
    LOG_WARN("failed to resolve sort field", K(ret), K(field_name));
  } else if (OB_FAIL(add_sort_order_item(select_stmt, dsl_sort_item))) {
    LOG_WARN("failed to add shorthand sort item", K(ret), K(field_name), K(dsl_sort_item));
  } else if (OB_FAIL(dsl_query_info_->sort_items_.push_back(dsl_sort_item))) {
    LOG_WARN("failed to push back shorthand sort item", K(ret), K(dsl_sort_item));
  }
  return ret;
}

int ObDSLResolver::resolve_sort_object_item(ObIJsonBase &sort_item, ObSelectStmt &select_stmt)
{
  int ret = OB_SUCCESS;
  if (sort_item.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sort item, expected exactly one field");
    LOG_WARN("sort item object must have exactly one key", K(ret));
  } else {
    ObString field_name;
    ObIJsonBase *field_opts = nullptr;
    ObDSLSortItem dsl_sort_item;
    if (OB_FAIL(sort_item.get_object_value(0, field_name, field_opts))) {
      LOG_WARN("fail to get sort item field", K(ret));
    } else if (OB_FAIL(resolve_sort_field(field_name, true, dsl_sort_item))) {
      LOG_WARN("failed to resolve sort field", K(ret), K(field_name));
    } else if (OB_FAIL(resolve_sort_options(field_opts, dsl_sort_item))) {
      LOG_WARN("failed to resolve sort options", K(ret), K(field_name));
    } else if (OB_FAIL(add_sort_order_item(select_stmt, dsl_sort_item))) {
      LOG_WARN("failed to add object sort item", K(ret), K(field_name), K(dsl_sort_item));
    } else if (OB_FAIL(dsl_query_info_->sort_items_.push_back(dsl_sort_item))) {
      LOG_WARN("failed to push back object sort item", K(ret), K(dsl_sort_item));
    }
  }
  return ret;
}

int ObDSLResolver::resolve_sort_options(ObIJsonBase *field_opts, ObDSLSortItem &dsl_sort_item)
{
  int ret = OB_SUCCESS;
  if (field_opts->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sort field options, expected a JSON object");
    LOG_WARN("sort field options must be an object", K(ret));
  } else {
    const uint64_t count = field_opts->element_count();
    for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      ObString key;
      ObIJsonBase *val = nullptr;
      if (OB_FAIL(field_opts->get_object_value(i, key, val))) {
        LOG_WARN("fail to get sort option key", K(ret), K(i));
      } else if (key.case_compare("order") == 0) {
        ObString order_str;
        if (val->json_type() != ObJsonNodeType::J_STRING) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sort.order, should be string");
          LOG_WARN("sort order must be string", K(ret));
        } else if (OB_FALSE_IT(order_str.assign_ptr(val->get_data(), val->get_data_length()))){
          LOG_WARN("failed to assign order string", K(ret));
        } else if (order_str.case_compare("desc") == 0) {
          dsl_sort_item.is_asc_ = false;
        } else if (order_str.case_compare("asc") == 0) {
          dsl_sort_item.is_asc_ = true;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sort.order, should be \"asc\" or \"desc\"");
          LOG_WARN("invalid sort order", K(ret), K(order_str));
        }
      } else if (key.case_compare("missing") == 0) {
        ObJsonNodeType json_type = val->json_type();
        if (json_type == ObJsonNodeType::J_NULL) {
          // "missing": null means no special handling, skip
        } else if (json_type == ObJsonNodeType::J_STRING) {
          ObString missing_str = ObString(val->get_data_length(), val->get_data());
          if (missing_str.case_compare("_first") == 0) {
            dsl_sort_item.missing_mode_ = ObDSLSortItem::MissingMode::FIRST;
          } else if (missing_str.case_compare("_last") == 0) {
            dsl_sort_item.missing_mode_ = ObDSLSortItem::MissingMode::LAST;
          } else if (OB_FAIL(resolve_sort_missing_literal(*val, dsl_sort_item))) {
            LOG_WARN("failed to resolve sort missing string literal", K(ret), KPC(val), K(dsl_sort_item));
          } else {
            dsl_sort_item.missing_mode_ = ObDSLSortItem::MissingMode::LITERAL;
          }
        } else if (val->is_json_scalar(json_type)) {
          if (OB_FAIL(resolve_sort_missing_literal(*val, dsl_sort_item))) {
            LOG_WARN("failed to resolve sort missing literal", K(ret), KPC(val), K(dsl_sort_item));
          } else {
            dsl_sort_item.missing_mode_ = ObDSLSortItem::MissingMode::LITERAL;
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sort.missing, should be a scalar value, \"_first\", or \"_last\"");
          LOG_WARN("sort missing value must be a supported scalar", K(ret), K(json_type));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "keys other than \"order\" and \"missing\" in sort");
        LOG_WARN("unsupported sort key", K(ret), K(key));
      }
    }
  }
  return ret;
}

int ObDSLResolver::build_sort_missing_string_literal(ObIJsonBase &missing_node, ObDSLSortItem &dsl_sort_item)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *lit = nullptr;
  if (missing_node.json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sort missing value for string column, expected a string literal");
    LOG_WARN("sort missing for string column must be string json", K(ret), K(missing_node.json_type()));
  } else if (OB_ISNULL(dsl_sort_item.field_expr_) || !dsl_sort_item.field_expr_->is_column_ref_expr()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sort field for string missing literal", K(ret), KP(dsl_sort_item.field_expr_));
  } else {
    const ObColumnRefRawExpr *col_expr = static_cast<const ObColumnRefRawExpr *>(dsl_sort_item.field_expr_);
    const ObObjType str_type = col_expr->get_result_type().get_type();
    const ObCollationType cs_type = col_expr->get_result_type().get_collation_type();
    const ObString str_val(missing_node.get_data_length(), missing_node.get_data());
    if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*params_.expr_factory_, str_type, str_val, cs_type, lit))) {
      LOG_WARN("failed to build const string for sort missing", K(ret), K(str_type), K(str_val));
    } else {
      dsl_sort_item.missing_literal_ = lit;
    }
  }
  return ret;
}

int ObDSLResolver::build_sort_missing_temporal_literal(ObIJsonBase &missing_node, ObDSLSortItem &dsl_sort_item)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *lit = nullptr;
  if (missing_node.json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sort missing value for temporal column, expected a string literal");
    LOG_WARN("sort missing for temporal column must be string json", K(ret), K(missing_node.json_type()));
  } else if (OB_ISNULL(dsl_sort_item.field_expr_) || !dsl_sort_item.field_expr_->is_column_ref_expr()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sort field for temporal missing literal", K(ret), KP(dsl_sort_item.field_expr_));
  } else {
    const ObColumnRefRawExpr *col_expr = static_cast<const ObColumnRefRawExpr *>(dsl_sort_item.field_expr_);
    const ObObjType col_type = col_expr->get_result_type().get_type();
    const ObString time_str(missing_node.get_data_length(), missing_node.get_data());
    ObDateSqlMode date_sql_mode;
    date_sql_mode.init(session_info_->get_sql_mode());
    date_sql_mode.allow_invalid_dates_ = false;
    const ObTimeZoneInfo *tz_info = session_info_->get_timezone_info();
    if (col_type == ObDateType) {
      int32_t dval = 0;
      if (OB_FAIL(ObTimeConverter::str_to_date(time_str, dval, date_sql_mode))) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sort missing date string");
        LOG_WARN("failed to parse sort missing as date", K(ret), K(time_str));
      } else if (OB_FAIL(ObRawExprUtils::build_const_date_expr(*params_.expr_factory_, dval, lit))) {
        LOG_WARN("failed to build const date expr for sort missing", K(ret));
      }
    } else if (col_type == ObMySQLDateType) {
      ObMySQLDate mdate;
      if (OB_FAIL(ObTimeConverter::str_to_mdate(time_str, mdate, date_sql_mode))) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sort missing mysql date string");
        LOG_WARN("failed to parse sort missing as mysql date", K(ret), K(time_str));
      } else {
        ObObj obj;
        obj.set_mysql_date(mdate);
        if (OB_FAIL(ObRawExprUtils::build_const_obj_expr(*params_.expr_factory_, obj, lit))) {
          LOG_WARN("failed to build const mysql date expr for sort missing", K(ret));
        }
      }
    } else if (col_type == ObDateTimeType || col_type == ObTimestampType) {
      int64_t dt_val = 0;
      int16_t scale = 0;
      const bool is_ts = (col_type == ObTimestampType);
      ObTimeConvertCtx cvrt_ctx(tz_info, is_ts);
      if (OB_FAIL(ObTimeConverter::str_to_datetime(time_str, cvrt_ctx, dt_val, &scale, date_sql_mode))) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sort missing datetime string");
        LOG_WARN("failed to parse sort missing as datetime", K(ret), K(time_str));
      } else if (OB_FAIL(ObRawExprUtils::build_const_datetime_expr(*params_.expr_factory_, dt_val, lit))) {
        LOG_WARN("failed to build const datetime expr for sort missing", K(ret));
      }
    } else if (col_type == ObMySQLDateTimeType) {
      ObMySQLDateTime mdt;
      int16_t scale = 0;
      ObTimeConvertCtx cvrt_ctx(tz_info, false);
      if (OB_FAIL(ObTimeConverter::str_to_mdatetime(time_str, cvrt_ctx, mdt, &scale, date_sql_mode))) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sort missing mysql datetime string");
        LOG_WARN("failed to parse sort missing as mysql datetime", K(ret), K(time_str));
      } else {
        ObObj obj;
        obj.set_mysql_datetime(mdt);
        if (OB_FAIL(ObRawExprUtils::build_const_obj_expr(*params_.expr_factory_, obj, lit))) {
          LOG_WARN("failed to build const mysql datetime expr for sort missing", K(ret));
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "sort missing literal for this temporal column type");
      LOG_WARN("missing literal not supported for temporal column", K(ret), K(col_type));
    }
    if (OB_SUCC(ret)) {
      dsl_sort_item.missing_literal_ = lit;
    }
  }
  return ret;
}

int ObDSLResolver::resolve_sort_missing_literal(ObIJsonBase &missing_node, ObDSLSortItem &dsl_sort_item)
{
  int ret = OB_SUCCESS;
  dsl_sort_item.missing_literal_ = nullptr;
  if (OB_ISNULL(dsl_sort_item.field_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort field expr is null", K(ret));
  } else if (dsl_sort_item.is_score_sort_) {
    ObRawExpr *expr = nullptr;
    if (OB_FAIL(resolve_const(missing_node, expr, ObJsonNodeType::J_DOUBLE))) {
      LOG_WARN("failed to resolve sort missing literal for __score", K(ret));
    } else {
      dsl_sort_item.missing_literal_ = static_cast<ObConstRawExpr *>(expr);
    }
  } else if (!dsl_sort_item.field_expr_->is_column_ref_expr()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort missing literal only supports user columns", K(ret));
  } else {
    const ObColumnRefRawExpr *col_expr = static_cast<const ObColumnRefRawExpr *>(dsl_sort_item.field_expr_);
    const ObObjType col_type = col_expr->get_result_type().get_type();
    const ObObjTypeClass tc = ob_obj_type_class(col_type);
    ObConstRawExpr *lit = nullptr;
    if (ObStringTC == tc) {
      if (OB_FAIL(build_sort_missing_string_literal(missing_node, dsl_sort_item))) {
        LOG_WARN("failed to build string sort missing literal", K(ret));
      }
    } else if (ObDateTC == tc || ObDateTimeTC == tc || ObMySQLDateTimeTC == tc || ObMySQLDateTC == tc) {
      if (OB_FAIL(build_sort_missing_temporal_literal(missing_node, dsl_sort_item))) {
        LOG_WARN("failed to build temporal sort missing literal", K(ret));
      }
    } else if (ObIntTC == tc) {
      int64_t ivalue = 0;
      uint64_t unused_uint = 0;
      if (missing_node.json_type() == ObJsonNodeType::J_DOUBLE) {
        if (OB_FAIL(trunc_json_float_to_int(missing_node, false, ivalue, unused_uint))) {
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sort missing value for integer column, out of range");
          LOG_WARN("sort missing float value out of int64 range", K(ret));
        }
      } else if (OB_FAIL(missing_node.to_int(ivalue, true, true))) {
        LOG_WARN("failed to convert json to int for sort missing", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*params_.expr_factory_, col_type, ivalue, lit))) {
        LOG_WARN("failed to build const int for sort missing", K(ret), K(col_type));
      } else {
        dsl_sort_item.missing_literal_ = lit;
      }
    } else if (ObUIntTC == tc) {
      uint64_t uvalue = 0;
      int64_t unused_int = 0;
      if (missing_node.json_type() == ObJsonNodeType::J_DOUBLE) {
        if (OB_FAIL(trunc_json_float_to_int(missing_node, true, unused_int, uvalue))) {
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sort missing value for unsigned integer column, out of range");
          LOG_WARN("sort missing float value out of uint64 range", K(ret));
        }
      } else if (OB_FAIL(missing_node.to_uint(uvalue, false, true))) {
        LOG_WARN("failed to convert json to uint for sort missing", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObRawExprUtils::build_const_uint_expr(*params_.expr_factory_, col_type, uvalue, lit))) {
        LOG_WARN("failed to build const uint for sort missing", K(ret), K(col_type));
      } else {
        dsl_sort_item.missing_literal_ = lit;
      }
    } else if (ObFloatTC == tc) {
      double dv = 0.0;
      if (OB_FAIL(missing_node.to_double(dv))) {
        LOG_WARN("failed to convert json to double for sort missing", K(ret));
      } else if (isnan(dv) || isinf(dv)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sort missing value for float column, NaN/Inf not allowed");
        LOG_WARN("sort missing float value is nan or inf", K(ret), K(dv));
      } else if (OB_FAIL(ObRawExprUtils::build_const_float_expr(*params_.expr_factory_, col_type, static_cast<float>(dv), lit))) {
        LOG_WARN("failed to build const float for sort missing", K(ret), K(col_type));
      } else {
        dsl_sort_item.missing_literal_ = lit;
      }
    } else if (ObDoubleTC == tc) {
      double dv = 0.0;
      if (OB_FAIL(missing_node.to_double(dv))) {
        LOG_WARN("failed to convert json to double for sort missing", K(ret));
      } else if (isnan(dv) || isinf(dv)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sort missing value for double column, NaN/Inf not allowed");
        LOG_WARN("sort missing double value is nan or inf", K(ret), K(dv));
      } else if (OB_FAIL(ObRawExprUtils::build_const_double_expr(*params_.expr_factory_, col_type, dv, lit))) {
        LOG_WARN("failed to build const double for sort missing", K(ret), K(col_type));
      } else {
        dsl_sort_item.missing_literal_ = lit;
      }
    } else if (ObNumberTC == tc || ObDecimalIntTC == tc) {
      number::ObNumber num;
      if (OB_FAIL(missing_node.to_number(allocator_, num))) {
        LOG_WARN("failed to convert json to number for sort missing", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_const_number_expr(*params_.expr_factory_, ObNumberType, num, lit))) {
        LOG_WARN("failed to build const number for sort missing", K(ret), K(col_type));
      } else {
        dsl_sort_item.missing_literal_ = lit;
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "sort missing literal for this column type");
      LOG_WARN("missing literal not supported for column type", K(ret), K(col_type), K(tc));
    }
  }
  return ret;
}

int ObDSLResolver::resolve_sort_field(ObString field_name,
                                      bool validate_sort_type,
                                      ObDSLSortItem &dsl_sort_item)
{
  int ret = OB_SUCCESS;
  dsl_sort_item.is_score_sort_ = (0 == field_name.case_compare(OB_HYBRID_SEARCH_SCORE_COLUMN_NAME));
  dsl_sort_item.is_asc_ = !dsl_sort_item.is_score_sort_;
  if (dsl_sort_item.is_score_sort_) {
    if (dsl_query_info_->score_cols_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("score columns not available for __score sort", K(ret));
    } else {
      dsl_sort_item.field_expr_ = dsl_query_info_->score_cols_.at(dsl_query_info_->score_cols_.count() - 1);
    }
  } else {
    ObColumnRefRawExpr *col_expr = nullptr;
    if (OB_FAIL(get_user_column_expr(field_name, col_expr))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, field_name.length(), field_name.ptr(),
                       table_item_.get_table_name().length(), table_item_.get_table_name().ptr());
      }
      LOG_WARN("fail to resolve sort field", K(ret), K(field_name));
    } else if (validate_sort_type
               && (col_expr->get_result_type().is_lob()
                   || col_expr->get_result_type().is_json()
                   || col_expr->get_result_type().is_geometry()
                   || col_expr->get_result_type().is_roaringbitmap()
                   || col_expr->get_result_type().is_collection_sql_type()
                   || col_expr->get_result_type().is_user_defined_sql_type()
                   || ob_is_extend(col_expr->get_result_type().get_type()))) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "sort by this column type");
      LOG_WARN("unsupported sort column type", K(ret), K(field_name),
               K(col_expr->get_result_type().get_type()));
    } else {
      ObColumnIndexInfo *idx_info = nullptr;
      if (OB_FAIL(get_col_idx_info(col_expr->get_column_name(), idx_info))) {
        LOG_WARN("fail to get col idx info for sort field", K(ret), K(field_name));
      } else if (OB_NOT_NULL(idx_info)) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "sort by fulltext/vector index column");
        LOG_WARN("sort on domain index column not supported", K(ret), K(field_name));
      } else if (validate_sort_type
                 && (col_expr->get_result_type().is_lob()
                     || col_expr->get_result_type().is_json()
                     || col_expr->get_result_type().is_geometry())) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "sort by text/json/geometry column type");
        LOG_WARN("unsupported sort column type", K(ret), K(field_name));
      } else {
        dsl_sort_item.field_expr_ = col_expr;
      }
    }
  }
  return ret;
}

int ObDSLResolver::add_sort_order_item(ObSelectStmt &select_stmt, const ObDSLSortItem &dsl_sort_item)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  ObRawExpr *sort_expr = dsl_sort_item.field_expr_;
  if (OB_ISNULL(sort_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort expr is null", K(ret), K(dsl_sort_item));
  } else if (dsl_sort_item.missing_mode_ == ObDSLSortItem::MissingMode::LITERAL
             && !dsl_sort_item.is_score_sort_) {
    ObSysFunRawExpr *coalesce_expr = nullptr;
    if (OB_ISNULL(dsl_sort_item.missing_literal_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("missing literal is null", K(ret), K(dsl_sort_item));
    } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SYS_COALESCE, coalesce_expr))) {
      LOG_WARN("fail to create coalesce expr", K(ret));
    } else if (OB_FAIL(coalesce_expr->set_param_exprs(dsl_sort_item.field_expr_,
                                                      static_cast<ObRawExpr *>(dsl_sort_item.missing_literal_)))) {
      LOG_WARN("fail to set coalesce params", K(ret));
    } else {
      coalesce_expr->set_func_name(ObString::make_string("coalesce"));
      sort_expr = coalesce_expr;
    }
  }
  if (OB_SUCC(ret)) {
    ObOrderDirection direction = NULLS_LAST_ASC;
    if (dsl_sort_item.missing_mode_ == ObDSLSortItem::MissingMode::FIRST) {
      direction = dsl_sort_item.is_asc_ ? NULLS_FIRST_ASC : NULLS_FIRST_DESC;
    } else {
      direction = dsl_sort_item.is_asc_ ? NULLS_LAST_ASC : NULLS_LAST_DESC;
    }
    OrderItem order_item(sort_expr, direction);
    if (OB_FAIL(select_stmt.add_order_item(order_item))) {
      LOG_WARN("fail to add order item", K(ret));
    }
  }
  return ret;
}

void ObDSLResolver::set_track_score(ObDSLQueryInfo *dsl_query_info)
{
  dsl_query_info->track_score_ =
      (dsl_query_info->result_mode_ != ObDSLResultMode::COUNT_AGG
       && dsl_query_info->result_mode_ != ObDSLResultMode::BUCKET_AGG);
}

int ObDSLResolver::refresh_output_score_after_select_resolved(const ObSelectStmt *select_stmt,
                                                              ObDSLQueryInfo *dsl_query_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(dsl_query_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(select_stmt), KP(dsl_query_info));
  } else {
    dsl_query_info->output_score_ = false;
    for (int64_t i = 0; !dsl_query_info->output_score_ && i < select_stmt->get_select_item_size(); ++i) {
      const SelectItem &select_item = select_stmt->get_select_item(i);
      dsl_query_info->output_score_ =
          (0 == select_item.expr_name_.case_compare(OB_HYBRID_SEARCH_SCORE_COLUMN_NAME));
    }
  }
  return ret;
}

int ObDSLResolver::init_result_mode_and_track_score()
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *select_stmt = nullptr;
  if (dsl_query_info_->result_mode_ != ObDSLResultMode::SEARCH_HITS) {
  } else if (FALSE_IT(select_stmt = static_cast<const ObSelectStmt *>(stmt_))) {
  } else if (!select_stmt->is_scala_group_by() || select_stmt->get_aggr_item_size() != 1) {
  } else {
    const ObAggFunRawExpr *aggr_expr = select_stmt->get_aggr_item(0);
    if (OB_ISNULL(aggr_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggregate expr is null", K(ret));
    } else if (aggr_expr->get_expr_type() == T_FUN_COUNT) {
      dsl_query_info_->result_mode_ = ObDSLResultMode::COUNT_AGG;
    }
  }
  if (OB_SUCC(ret) && dsl_query_info_->has_dsl_aggs()) {
    const ObDSLAggTermsItem &agg_item = dsl_query_info_->agg_items_.at(0);
    if (agg_item.agg_type_ == ObDSLAggTermsItem::TERMS) {
      dsl_query_info_->result_mode_ = ObDSLResultMode::BUCKET_AGG;
    } else if (agg_item.agg_type_ == ObDSLAggTermsItem::CARDINALITY) {
      dsl_query_info_->result_mode_ = ObDSLResultMode::COUNT_AGG;
    }
  }
  if (OB_SUCC(ret)) {
    set_track_score(dsl_query_info_);
    dsl_query_info_->output_score_ = false;
  }
  return ret;
}

int ObDSLResolver::refresh_result_mode_after_select_resolved(const ObSelectStmt *select_stmt, ObDSLQueryInfo *dsl_query_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(dsl_query_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(select_stmt), KP(dsl_query_info));
  } else {
    if (dsl_query_info->result_mode_ != ObDSLResultMode::SEARCH_HITS) {
    } else if (!select_stmt->is_scala_group_by() || select_stmt->get_aggr_item_size() != 1) {
    } else {
      const ObAggFunRawExpr *aggr_expr = select_stmt->get_aggr_item(0);
      if (OB_ISNULL(aggr_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("aggregate expr is null", K(ret));
      } else if (aggr_expr->get_expr_type() == T_FUN_COUNT) {
        dsl_query_info->result_mode_ = ObDSLResultMode::COUNT_AGG;
      }
    }
    if (OB_SUCC(ret) && dsl_query_info->has_dsl_aggs()) {
      const ObDSLAggTermsItem &agg_item = dsl_query_info->agg_items_.at(0);
      if (agg_item.agg_type_ == ObDSLAggTermsItem::TERMS) {
        dsl_query_info->result_mode_ = ObDSLResultMode::BUCKET_AGG;
      } else if (agg_item.agg_type_ == ObDSLAggTermsItem::CARDINALITY) {
        dsl_query_info->result_mode_ = ObDSLResultMode::COUNT_AGG;
      }
    }
    if (OB_SUCC(ret)) {
      set_track_score(dsl_query_info);
    }
    if (OB_SUCC(ret)
        && OB_FAIL(refresh_output_score_after_select_resolved(select_stmt, dsl_query_info))) {
      LOG_WARN("failed to refresh output score after select resolved", K(ret));
    }
  }
  return ret;
}

int ObDSLResolver::resolve_boost(ObIJsonBase &req_node, ObConstRawExpr *&boost_expr, ObEsQueryItem query_type, ObEsQueryItem outer_query_type)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = nullptr;
  double boost_value = 0.0;
  const uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();
  if (!HYBRID_SEARCH_SUPPORT_SCALAR_SCORING(cluster_version) &&
      (IS_QUERY_ITEM_ARRAY(query_type) ||
       IS_QUERY_ITEM_JSON(query_type) ||
       IS_QUERY_ITEM_SCALAR(query_type))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("boost value not supported in this version", K(ret), K(query_type), K(cluster_version));
  } else if (OB_FAIL(resolve_const(req_node, expr, ObJsonNodeType::J_DOUBLE))) {
    LOG_WARN("fail to resolve boost value", K(ret));
  } else if (OB_ISNULL(boost_expr = static_cast<ObConstRawExpr*>(expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("boost expr must be const expr", K(ret));
  } else if (OB_FAIL(boost_expr->get_value().get_double(boost_value))) {
    LOG_WARN("fail to get double value from boost expr", K(ret));
  } else if (boost_value < 0.0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "boost value, it must not be negative");
    LOG_WARN("boost value must be greater than or equal to 0", K(ret), K(boost_value));
  } else if (boost_value == 0.0 &&
             (query_type == QUERY_ITEM_BOOL ||
              (IS_QUERY_ITEM_FULLTEXT(query_type) && outer_query_type != QUERY_ITEM_QUERY))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "boost value, it must be positive when it is in fulltext query at non-top level or in bool query");
    LOG_WARN("boost value should be positive", K(ret));
  }
  return ret;
}

int ObDSLResolver::resolve_match(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  ObString col_name;
  ObIJsonBase *col_para = nullptr;
  ObIJsonBase *query_node = nullptr;
  const ObTableSchema *index_schema = nullptr;
  ObColumnRefRawExpr *col_expr = nullptr;
  ObRawExpr *query_expr = nullptr;
  ObConstRawExpr *boost_expr = nullptr;
  ObConstRawExpr *min_should_match_expr = nullptr;
  ObConstRawExpr *operator_expr = nullptr;
  bool has_msm_key = false;
  ObIJsonBase *match_msm_json_node = nullptr;
  ObMatchOperator match_operator = ObDSLMatchQuery::DEFAULT_OPERATOR;
  ObCollationType collation_type = CS_TYPE_INVALID;
  ObDSLMatchQuery *match_query = nullptr;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT || req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "match query, should be single-key object");
    LOG_WARN("match query should be single-key object", K(ret));
  } else if (OB_FAIL(req_node.get_object_value(0, col_name, col_para))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (OB_FAIL(get_user_column_expr(col_name, col_expr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, col_name.length(), col_name.ptr(),
                     table_item_.get_table_name().length(), table_item_.get_table_name().ptr());
    }
    LOG_WARN("fail to get user column expr", K(ret), K(col_name));
  } else if (OB_FAIL(get_fulltext_index_schema(col_name, index_schema))) {
    LOG_WARN("fail to get fulltext index schema", K(ret), K(col_name));
  } else if (col_para->json_type() == ObJsonNodeType::J_STRING) {
    query_node = col_para;
  } else if (col_para->json_type() == ObJsonNodeType::J_OBJECT) {
    for (uint64_t i = 0; OB_SUCC(ret) && i < col_para->element_count(); i++) {
      ObString key;
      ObIJsonBase *sub_node = nullptr;
      if (OB_FAIL(col_para->get_object_value(i, key, sub_node))) {
        LOG_WARN("fail to get value", K(ret));
      } else if (key.case_compare("query") == 0) {
        if (sub_node->json_type() != ObJsonNodeType::J_STRING) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query in match query");
          LOG_WARN("match query text must be string type", K(ret), K(sub_node->json_type()));
        } else {
          query_node = sub_node;
        }
      } else if (key.case_compare("boost") == 0) {
        if (OB_FAIL(resolve_boost(*sub_node, boost_expr, QUERY_ITEM_MATCH, outer_query_type))) {
          LOG_WARN("fail to resolve boost", K(ret));
        }
      } else if (key.case_compare("minimum_should_match") == 0) {
        has_msm_key = true;
        match_msm_json_node = sub_node;
      } else if (key.case_compare("operator") == 0) {
        if (OB_FAIL(resolve_query_string_operator(*sub_node, match_operator))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "operator in match query, should be \"or\" or \"and\"");
          LOG_WARN("fail to resolve query string operator", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Parameter in match query");
        LOG_WARN("unsupported key in match query", K(ret), K(key));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "match query field value, should be string or object");
    LOG_WARN("match query field value should be string or object", K(ret), K(col_para->json_type()));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(query_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "match query, required param \"query\" is missing");
    LOG_WARN("missing query node", K(ret));
  } else if (OB_FALSE_IT(collation_type = col_expr->get_collation_type())) {
  } else if (OB_FAIL(resolve_minimum_should_match_expr(
                     has_msm_key ? match_msm_json_node : nullptr, collation_type,
                     ObDSLMatchQuery::DEFAULT_MINIMUM_SHOULD_MATCH, min_should_match_expr))) {
    LOG_WARN("fail to resolve or default minimum should match expr", KR(ret));
  } else if (OB_ISNULL(min_should_match_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("minimum should match expr is null", K(ret));
  } else if (OB_FAIL(resolve_query_string_expr(
                     ObString(query_node->get_data_length(), query_node->get_data()),
                     collation_type, query_expr))) {
    LOG_WARN("fail to construct string expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, static_cast<int64_t>(match_operator), operator_expr))) {
    LOG_WARN("fail to build const int expr for operator", K(ret));
  } else if (OB_FAIL(ObDSLMatchQuery::create(outer_query_type, parent_query, *allocator_, match_query))) {
    LOG_WARN("fail to create match query", K(ret));
  } else {
    match_query->query_ = static_cast<ObConstRawExpr*>(query_expr);
    match_query->boost_ = setup_boost(boost_expr);
    match_query->field_ = col_expr;
    match_query->minimum_should_match_ = min_should_match_expr;
    match_query->operator_ = operator_expr;
    query = match_query;
  }
  return ret;
}

int ObDSLResolver::resolve_match_phrase(ObIJsonBase &req_node,
                                        ObDSLQuery *&query,
                                        ObDSLQuery *parent_query,
                                        ObEsQueryItem outer_query_type)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  ObString col_name;
  ObIJsonBase *col_para = nullptr;
  ObIJsonBase *query_node = nullptr;
  const ObTableSchema *index_schema = nullptr;
  ObColumnRefRawExpr *col_expr = nullptr;
  ObRawExpr *query_expr = nullptr;
  ObConstRawExpr *boost_expr = nullptr;
  ObConstRawExpr *slop_expr = nullptr;
  int32_t slop = ObDSLMatchPhraseQuery::DEFAULT_SLOP;
  ObCollationType collation_type = CS_TYPE_INVALID;
  ObDSLMatchPhraseQuery *match_phrase_query = nullptr;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT || req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "match_phrase query, should be single-key object");
    LOG_WARN("match_phrase query should be single-key object", K(ret));
  } else if (OB_FAIL(req_node.get_object_value(0, col_name, col_para))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (OB_FAIL(get_user_column_expr(col_name, col_expr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, col_name.length(), col_name.ptr(),
                     table_item_.get_table_name().length(), table_item_.get_table_name().ptr());
    }
    LOG_WARN("fail to get user column expr", K(ret), K(col_name));
  } else if (OB_FAIL(get_fulltext_index_schema(col_name, index_schema))) {
    LOG_WARN("fail to get fulltext index schema", K(ret), K(col_name));
  } else if (ObJsonNodeType::J_STRING == col_para->json_type()) {
    query_node = col_para;
  } else if (ObJsonNodeType::J_OBJECT == col_para->json_type()) {
    for (uint64_t i = 0; OB_SUCC(ret) && i < col_para->element_count(); i++) {
      ObString key;
      ObIJsonBase *sub_node = nullptr;
      if (OB_FAIL(col_para->get_object_value(i, key, sub_node))) {
        LOG_WARN("fail to get value", K(ret));
      } else if (key.case_compare("query") == 0) {
        if (sub_node->json_type() != ObJsonNodeType::J_STRING) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query in match_phrase query");
          LOG_WARN("match query text must be string type", K(ret), K(sub_node->json_type()));
        } else {
          query_node = sub_node;
        }
      } else if (key.case_compare("boost") == 0) {
        if (OB_FAIL(resolve_boost(*sub_node, boost_expr, QUERY_ITEM_MATCH_PHRASE, outer_query_type))) {
          LOG_WARN("fail to resolve boost", K(ret));
        }
      } else if (key.case_compare("slop") == 0) {
        if (OB_FAIL(resolve_slop(*sub_node, slop))) {
          LOG_WARN("fail to resolve slop", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Parameter in match_phrase query");
        LOG_WARN("unsupported key in match_phrase query", K(ret), K(key));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "match_phrase query field value, should be string or object");
    LOG_WARN("match_phrase query field value should be string or object", K(ret), K(col_para->json_type()));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(query_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "match_phrase query, required param \"query\" is missing");
    LOG_WARN("missing query node", K(ret));
  } else if (OB_FALSE_IT(collation_type = col_expr->get_collation_type())) {
  } else if (OB_FAIL(resolve_query_string_expr(ObString(query_node->get_data_length(), query_node->get_data()), collation_type, query_expr))) {
    LOG_WARN("fail to construct string expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, slop, slop_expr))) {
    LOG_WARN("fail to build const int expr for slop", K(ret));
  } else if (OB_FAIL(ObDSLMatchPhraseQuery::create(outer_query_type, parent_query, *allocator_, match_phrase_query))) {
    LOG_WARN("fail to create match phrase query", K(ret));
  } else {
    match_phrase_query->field_ = col_expr;
    match_phrase_query->query_ = static_cast<ObConstRawExpr*>(query_expr);
    match_phrase_query->boost_ = setup_boost(boost_expr);
    match_phrase_query->slop_ = slop_expr;
    query = match_phrase_query;
  }
  return ret;
}

int ObDSLResolver::resolve_knn(ObIJsonBase &req_node, ObDSLQuery *&query)
{
  int ret = OB_SUCCESS;
  const char *params_name[] = {"field", "k", "query_vector"};
  RequiredParamsSet required_params;
  ObVectorIndexDistAlgorithm dist_algo = ObVectorIndexDistAlgorithm::VIDA_L2;
  ObColumnRefRawExpr *field_expr = nullptr;
  ObRawExpr *k_expr = nullptr;
  ObRawExpr *query_vector_expr = nullptr;
  ObRawExpr *distance_expr = nullptr;
  int64_t k_value = 0;
  int64_t num_candidates = 0;
  bool has_num_candidates = false;
  ObSEArray<ObDSLQuery*, 4, ModulePageAllocator, true> filter_queries;
  ObConstRawExpr *boost_expr = nullptr;
  ObDSLKnnQuery::SearchOption *search_option = nullptr;
  ObDSLKnnQuery *knn_query = nullptr;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "knn, should be object");
    LOG_WARN("knn should be object", K(ret), K(req_node.json_type()));
  } else if (OB_FAIL(construct_required_params(params_name, sizeof(params_name)/sizeof(params_name[0]), required_params))) {
    LOG_WARN("fail to create required params set", K(ret));
  } else if (OB_FAIL(ObDSLKnnQuery::create(*allocator_, knn_query, QUERY_ITEM_UNKNOWN))) {
    LOG_WARN("fail to create knn query", K(ret));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < req_node.element_count(); i++) {
    ObString key;
    ObIJsonBase *sub_node = nullptr;
    if (OB_FAIL(req_node.get_object_value(i, key, sub_node))) {
      LOG_WARN("fail to get value", K(ret), K(i));
    } else if (key.case_compare("field") == 0) {
      ObString col_name;
      if (sub_node->json_type() != ObJsonNodeType::J_STRING) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "knn.field, should be string");
        LOG_WARN("knn.field should be string", K(ret), K(sub_node->json_type()));
      } else if (OB_FALSE_IT(col_name.assign_ptr(sub_node->get_data(), sub_node->get_data_length()))) {
      } else if (OB_FAIL(get_user_column_expr(col_name, field_expr))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "field, field in knn search not exists");
        }
        LOG_WARN("fail to get user column expr", K(ret), K(col_name));
      } else if (OB_FAIL(get_dist_algo_type(field_expr, dist_algo))) {
        LOG_WARN("fail to get vector index dist algorithm", K(ret));
      } else if (OB_FAIL(required_params.erase_refactored(ObString("field")))) {
        LOG_WARN("fail to erase set", K(ret));
      }
    } else if (key.case_compare("k") == 0) {
      ObConstRawExpr *const_expr = nullptr;
      if (OB_FAIL(resolve_const(*sub_node, k_expr, ObJsonNodeType::J_INT))) {
        LOG_WARN("fail to resolve k constant", K(ret));
      } else if (OB_ISNULL(const_expr = static_cast<ObConstRawExpr*>(k_expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("k expr must be const expr", K(ret));
      } else if (OB_FAIL(const_expr->get_value().get_int(k_value))) {
        LOG_WARN("fail to get int value from k expr", K(ret));
      } else if (k_value <= 0 || k_value > ObDSLResolver::KNN_K_VALUE_MAX) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "k, k value should be in range [1, 16384]");
        LOG_WARN("k value must be in range [1, 16384]", K(ret), K(k_value));
      } else if (OB_FAIL(required_params.erase_refactored(ObString("k")))) {
        LOG_WARN("fail to erase set", K(ret));
      }
    } else if (key.case_compare("query_vector") == 0) {
      if (OB_FAIL(resolve_const(*sub_node, query_vector_expr, ObJsonNodeType::J_STRING))) {
        LOG_WARN("fail to resolve query vector", K(ret));
      } else if (OB_FAIL(required_params.erase_refactored(ObString("query_vector")))) {
        LOG_WARN("fail to erase set", K(ret));
      }
    } else if (key.case_compare("num_candidates") == 0) {
      if (has_num_candidates) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "duplicate field num_candidates");
        LOG_WARN("duplicate field num_candidates", K(ret));
      } else if (sub_node->json_type() != ObJsonNodeType::J_INT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("num_candidates should be int type", K(ret), K(sub_node->json_type()));
      } else if (OB_FAIL(sub_node->to_int(num_candidates))) {
        LOG_WARN("fail to get int value from num_candidates", K(ret));
      } else if (OB_ISNULL(search_option) &&
                 OB_ISNULL(search_option = OB_NEWx(ObDSLKnnQuery::SearchOption, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for search option", K(ret));
      } else {
        has_num_candidates = true;
      }
    } else if (key.case_compare("boost") == 0) {
      if (OB_FAIL(resolve_boost(*sub_node, boost_expr, QUERY_ITEM_KNN, QUERY_ITEM_UNKNOWN))) {
        LOG_WARN("fail to resolve boost", K(ret));
      }
    } else if (key.case_compare("similarity") == 0) {
      double similarity = 0.0;
      ObJsonNodeType json_type = sub_node->json_type();
      if (!ObIJsonBase::is_json_number_type(json_type) && json_type != ObJsonNodeType::J_STRING) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "similarity, should be number or string type");
        LOG_WARN("similarity should be number or string type", K(ret), K(json_type));
      } else if (OB_FAIL(sub_node->to_double(similarity))) {
        if (json_type == ObJsonNodeType::J_STRING) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "similarity, invalid numeric string");
        }
        LOG_WARN("fail to get double value from similarity", K(ret));
      } else if (similarity < 0.0 || similarity > 1.0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "similarity, should be in range [0.0, 1.0]");
        LOG_WARN("similarity should be in range [0.0, 1.0]", K(ret), K(similarity));
      } else if (OB_ISNULL(search_option) &&
                 OB_ISNULL(search_option = OB_NEWx(ObDSLKnnQuery::SearchOption, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for search option", K(ret));
      } else {
        search_option->param_.is_set_similarity_threshold_ = 1;
        search_option->param_.similarity_threshold_ = similarity;
      }
    } else if (key.case_compare("filter") == 0) {
      int64_t filter_count = 0;
      if (OB_FAIL(resolve_bool_clause(*sub_node, filter_queries, filter_count, knn_query, QUERY_ITEM_FILTER))) {
        LOG_WARN("fail to resolve filter clauses", K(ret));
      }
    } else if (key.case_compare("search_options") == 0) {
      if (OB_FAIL(resolve_search_options(*sub_node, search_option))) {
        LOG_WARN("fail to resolve search option", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid key in knn query");
      LOG_WARN("not supported syntax in knn query", K(ret), K(key));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!required_params.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "knn query, required params \"field\", \"query_vector\", \"k\" are missing");
    LOG_WARN("knn required params are missing", K(ret), K(required_params.begin()->first));
  } else if (has_num_candidates && (num_candidates < k_value || num_candidates > 10000)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "num_candidates, num_candidates value should be in range [k, 10000]");
    LOG_WARN("num_candidates value should be in range [k, 10000]", K(ret), K(num_candidates), K(k_value));
  } else if (OB_NOT_NULL(search_option) &&
             search_option->param_.is_set_similarity_threshold_ == 1 &&
             dist_algo == ObVectorIndexDistAlgorithm::VIDA_IP) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "similarity in ip distance");
    LOG_WARN("similarity in ip distance is not supported", K(ret));
  } else if (OB_FAIL(knn_query->filter_.assign(filter_queries))) {
    LOG_WARN("fail to assign filter queries", K(ret));
  } else if (OB_FAIL(construct_dist_expr(field_expr, query_vector_expr, dist_algo, distance_expr))) {
    LOG_WARN("fail to construct distance expr", K(ret));
  } else {
    if (has_num_candidates) {
      search_option->is_set_num_candidates_ = true;
      search_option->num_candidates_ = static_cast<int32_t>(num_candidates);
      search_option->param_.ef_search_ = static_cast<int32_t>(num_candidates);
      search_option->param_.is_set_ef_search_ = 1;
    }
    knn_query->dist_algo_ = dist_algo;
    knn_query->field_ = field_expr;
    knn_query->k_ = static_cast<ObConstRawExpr*>(k_expr);
    knn_query->query_vector_ = static_cast<ObConstRawExpr*>(query_vector_expr);
    knn_query->distance_ = distance_expr;
    knn_query->boost_ = setup_boost(boost_expr);
    knn_query->search_option_ = search_option;
    query = knn_query;
  }
  return ret;
}

int ObDSLResolver::resolve_bool(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  ObSEArray<ObDSLQuery*, 4, ModulePageAllocator, true> must;
  ObSEArray<ObDSLQuery*, 4, ModulePageAllocator, true> should;
  ObSEArray<ObDSLQuery*, 4, ModulePageAllocator, true> filter;
  ObSEArray<ObDSLQuery*, 4, ModulePageAllocator, true> must_not;
  int64_t must_cnt = -1;
  int64_t should_cnt = -1;
  int64_t filter_cnt = -1;
  int64_t must_not_cnt = -1;
  int64_t score_cnt = 0;
  ObConstRawExpr *msm_expr = nullptr;
  ObConstRawExpr *boost_expr = nullptr;
  ObDSLBoolQuery *bool_query = nullptr;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "bool query, should be object");
    LOG_WARN("bool query should be object", K(ret), K(req_node.json_type()));
  } else if (OB_FALSE_IT(count = req_node.element_count())) {
  } else if (OB_FAIL(init_bool_info(req_node, msm_expr, boost_expr))) {
    LOG_WARN("fail to init bool info", K(ret));
  } else if (OB_FAIL(ObDSLBoolQuery::create(*allocator_, bool_query, outer_query_type, parent_query))) {
    LOG_WARN("fail to create bool query", K(ret));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    ObString key;
    ObIJsonBase *sub_node = nullptr;
    if (OB_FAIL(req_node.get_object_value(i, key, sub_node))) {
      LOG_WARN("fail to get value", K(ret), K(i));
    } else if (key.case_compare("must") == 0) {
      if (OB_FAIL(resolve_bool_clause(*sub_node, must, must_cnt, bool_query, QUERY_ITEM_MUST))) {
        LOG_WARN("fail to resolve must clauses", K(ret), K(i));
      }
    } else if (key.case_compare("should") == 0) {
      if (OB_FAIL(resolve_bool_clause(*sub_node, should, should_cnt, bool_query, QUERY_ITEM_SHOULD))) {
        LOG_WARN("fail to resolve should clauses", K(ret), K(i));
      }
    } else if (key.case_compare("filter") == 0) {
      if (OB_FAIL(resolve_bool_clause(*sub_node, filter, filter_cnt, bool_query, QUERY_ITEM_FILTER))) {
        LOG_WARN("fail to resolve filter clauses", K(ret), K(i));
      }
    } else if (key.case_compare("must_not") == 0) {
      if (OB_FAIL(resolve_bool_clause(*sub_node, must_not, must_not_cnt, bool_query, QUERY_ITEM_MUST_NOT))) {
        LOG_WARN("fail to resolve must_not clauses", K(ret), K(i));
      }
    } else {} // other keys are resolved in init_bool_info()
  }
  if (OB_FAIL(ret)) {
  } else if (must.count() + should.count() + filter.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "bool query, it should have at least one positive clause");
    LOG_WARN("bool query must have at least one positive clause", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < must.count(); i++) {
    if (must.at(i)->need_cal_score_) {
      score_cnt++;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < should.count(); i++) {
    if (should.at(i)->need_cal_score_) {
      score_cnt++;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(bool_query->must_.assign(must))) {
    LOG_WARN("fail to assign must queries", K(ret));
  } else if (OB_FAIL(bool_query->should_.assign(should))) {
    LOG_WARN("fail to assign should queries", K(ret));
  } else if (OB_FAIL(bool_query->filter_.assign(filter))) {
    LOG_WARN("fail to assign filter queries", K(ret));
  } else if (OB_FAIL(bool_query->must_not_.assign(must_not))) {
    LOG_WARN("fail to assign must_not queries", K(ret));
  } else {
    bool_query->must_cnt_ = must_cnt;
    bool_query->should_cnt_ = should_cnt;
    bool_query->filter_cnt_ = filter_cnt;
    bool_query->must_not_cnt_ = must_not_cnt;
    bool_query->minimum_should_match_ = msm_expr;
    int32_t msm_snapshot = 1;
    if (OB_FAIL(ObDSLResolver::dsl_bool_msm_snapshot_from_msm_expr(msm_expr, msm_snapshot))) {
      LOG_WARN("failed to derive bool msm snapshot", K(ret));
    } else {
      bool_query->msm_ = msm_snapshot;
    }
    if (OB_SUCC(ret)) {
      bool_query->boost_ = setup_boost(boost_expr);
      // score is true only if there is at least one item which really needs to calculate score
      bool_query->need_cal_score_ = bool_query->need_cal_score_ && score_cnt > 0;
      query = bool_query;
    }
  }
  return ret;
}

int ObDSLResolver::resolve_bool_clause(ObIJsonBase &req_node, ObIArray<ObDSLQuery*> &queries, int64_t &count, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type)
{
  int ret = OB_SUCCESS;
  uint64_t element_count = 0;
  ObIJsonBase *clause_val = nullptr;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT &&
      req_node.json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "bool inner clause, should be object or array");
    LOG_WARN("bool inner clause should be object or array", K(ret), K(req_node.json_type()));
  } else if (OB_FALSE_IT(element_count = req_node.element_count())) {
  } else if (element_count == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "bool inner clause, it cannot be empty");
    LOG_WARN("empty bool clause", K(ret), K(outer_query_type));
  } else if (req_node.json_type() == ObJsonNodeType::J_OBJECT && element_count != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "bool inner clause, it should contain only one key when it is an object");
    LOG_WARN("invalid bool inner clause", K(ret), K(outer_query_type));
  } else {
    count = element_count;
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < element_count; i++) {
    ObDSLQuery *sub_query = nullptr;
    if (req_node.json_type() == ObJsonNodeType::J_OBJECT) {
      clause_val = &req_node;
    } else if (OB_FAIL(req_node.get_array_element(i, clause_val))) {
      LOG_WARN("unexpectd json type", K(ret), K(i));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(resolve_single_term(*clause_val, sub_query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve bool clause sub query", K(ret), K(i));
    } else if (OB_FAIL(queries.push_back(sub_query))) {
      LOG_WARN("fail to push back bool clause query", K(ret));
    }
  }
  return ret;
}

// Type conversion matrix: [json_type][target_type] -> allowed (true/false)
// json_type \ target_type | J_STRING | J_INT | J_UINT | J_DOUBLE | J_ARRAY | J_OBJECT | J_BOOLEAN
// ------------------------|----------|-------|--------|----------|---------|----------|----------
// J_STRING                |   true   | true  | false  |  true    |  false  |   true   |   false
// J_INT                   |   false  | true  | false  |  true    |  false  |   false  |   false
// J_UINT                  |   false  | true  | true   |  false   |  false  |   false  |   false
// J_DOUBLE                |   false  | true  | false  |  true    |  false  |   false  |   false
// J_ARRAY                 |   true   | false | false  |  false   |  true   |   true   |   false
// J_OBJECT                |   true   | false | false  |  false   |  false  |   true   |   false
// J_BOOLEAN               |   true   | false | false  |  false   |  false  |   false  |   true
int ObDSLResolver::resolve_const(ObIJsonBase &req_node,
                                 ObRawExpr *&expr,
                                 ObJsonNodeType target_type,
                                 ObEsQueryItem query_type/*=QUERY_ITEM_UNKNOWN*/,
                                 ObObjType array_base_type/*=ObMaxType*/)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  ObJsonNodeType json_type = req_node.json_type();
  bool can_cast = false;
  if (target_type == ObJsonNodeType::J_STRING) {
    can_cast = (json_type == ObJsonNodeType::J_STRING ||
                json_type == ObJsonNodeType::J_ARRAY ||
                json_type == ObJsonNodeType::J_OBJECT ||
                json_type == ObJsonNodeType::J_BOOLEAN);
  } else if (target_type == ObJsonNodeType::J_UINT) {
    can_cast = (json_type == ObJsonNodeType::J_UINT);
  } else if (target_type == ObJsonNodeType::J_INT ||
             target_type == ObJsonNodeType::J_DOUBLE) {
    can_cast = (req_node.is_json_number(json_type) || (json_type == ObJsonNodeType::J_STRING));
  } else if (target_type == ObJsonNodeType::J_ARRAY) {
    can_cast = (json_type == ObJsonNodeType::J_ARRAY);
  } else if (target_type == ObJsonNodeType::J_OBJECT) {
    can_cast = (json_type == ObJsonNodeType::J_OBJECT);
  } else if (target_type == ObJsonNodeType::J_BOOLEAN) {
    can_cast = (json_type == ObJsonNodeType::J_BOOLEAN || json_type == ObJsonNodeType::J_STRING);
  }
  if (!can_cast) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unsupported conversion from source type to target type", K(ret), K(json_type), K(target_type));
  } else if (target_type == ObJsonNodeType::J_STRING ||
             target_type == ObJsonNodeType::J_OBJECT) {
    ObString str_val;
    if (json_type == ObJsonNodeType::J_STRING) {
      str_val = ObString(req_node.get_data_length(), req_node.get_data());
    } else if (json_type == ObJsonNodeType::J_ARRAY || json_type == ObJsonNodeType::J_OBJECT) {
      ObJsonBuffer j_buffer(allocator_);
      if (OB_FAIL(req_node.print(j_buffer, false))) {
        LOG_WARN("fail to serialize json to string", K(ret));
      } else if (OB_FAIL(j_buffer.get_result_string(str_val))) {
        LOG_WARN("fail to get result string", K(ret));
      }
    } else if (json_type == ObJsonNodeType::J_BOOLEAN) {
      str_val = req_node.get_boolean() ? ObString::make_string("true") : ObString::make_string("false");
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(construct_string_expr(str_val, expr))) {
      LOG_WARN("fail to create const string expr", K(ret));
    } else if (target_type == ObJsonNodeType::J_OBJECT && query_type == QUERY_ITEM_JSON_MEMBER_OF) {
      ObSysFunRawExpr *cast_expr = nullptr;
      ObRawExprResType json_dst_type;
      json_dst_type.set_type(ObJsonType);
      json_dst_type.set_length(ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType].get_length());
      json_dst_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      if (OB_FAIL(ObRawExprUtils::create_cast_expr(*expr_factory, expr, json_dst_type, cast_expr, session_info_))) {
        LOG_WARN("fail to create cast to json expr", K(ret));
      } else {
        expr = cast_expr;
      }
    }
  } else if (target_type == ObJsonNodeType::J_INT ||
             target_type == ObJsonNodeType::J_UINT ||
             target_type == ObJsonNodeType::J_DOUBLE) {
    ObConstRawExpr *const_expr = nullptr;
    if (target_type == ObJsonNodeType::J_INT) {
      if (OB_FAIL(resolve_const_to_int(req_node, const_expr))) {
        LOG_WARN("fail to resolve int const", K(ret));
      }
    } else if (target_type == ObJsonNodeType::J_UINT) {
      if (OB_FAIL(resolve_const_to_uint(req_node, const_expr))) {
        LOG_WARN("fail to resolve uint const", K(ret));
      }
    } else { // target_type == ObJsonNodeType::J_DOUBLE
      if (OB_FAIL(resolve_const_to_double(req_node, const_expr, array_base_type))) {
        LOG_WARN("fail to resolve double const", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      expr = const_expr;
    }
  } else if (target_type == ObJsonNodeType::J_ARRAY) {
    ObSysFunRawExpr *array_expr = nullptr;
    int64_t count = req_node.element_count();
    if (count == 0 && query_type != QUERY_ITEM_JSON_MEMBER_OF) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "array, should have at least one element");
      LOG_WARN("array must have at least one element", K(ret));
    } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SYS_ARRAY, array_expr))) {
      LOG_WARN("fail to create array func expr", K(ret));
    } else if (OB_FALSE_IT(array_expr->set_func_name(N_ARRAY))) {
    } else if (OB_FAIL(array_expr->init_param_exprs(count))) {
      LOG_WARN("fail to init param exprs", K(ret));
    } else {
      // resolve each array element recursively
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        ObIJsonBase *elem_node = nullptr;
        ObRawExpr *elem_expr = nullptr;
        ObJsonNodeType elem_json_type = ObJsonNodeType::J_NULL;
        if (OB_FAIL(req_node.get_array_element(i, elem_node))) {
          LOG_WARN("fail to get array element", K(ret), K(i));
        } else if (OB_FALSE_IT(elem_json_type = resolve_type_mapping(elem_node->json_type(), nullptr,
                                                                     query_type, array_base_type))) {
        } else if (OB_FAIL(resolve_const(*elem_node, elem_expr, elem_json_type, query_type, array_base_type))) {
          LOG_WARN("fail to resolve array element", K(ret), K(i));
        } else if (OB_FAIL(array_expr->add_param_expr(elem_expr))) {
          LOG_WARN("fail to add param expr", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        expr = array_expr;
      }
    }
  } else if (target_type == ObJsonNodeType::J_BOOLEAN) {
    bool b_value = true;
    if (json_type== ObJsonNodeType::J_BOOLEAN) {
      b_value = req_node.get_boolean();
    } else {
      ObString bool_str = ObString(req_node.get_data_length(), req_node.get_data()).trim();
      if (bool_str.case_compare("true") == 0) {
        b_value = true;
      } else if (bool_str.case_compare("false") == 0) {
        b_value = false;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "boolean value, should be \"true\" or \"false\" when it is a string");
        LOG_WARN("invalid boolean value", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(expr_factory, expr, b_value))) {
      LOG_WARN("fail to create const bool expr", K(ret));
    }
  }
  return ret;
}

int ObDSLResolver::resolve_const_to_double(ObIJsonBase &req_node, ObConstRawExpr *&const_expr, ObObjType array_base_type)
{
  int ret = OB_SUCCESS;
  double dval = 0.0;
  if (OB_FAIL(req_node.to_double(dval))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to convert json node to double", K(ret), K(req_node.json_type()));
  } else if (OB_FAIL(build_const_double(dval, const_expr, array_base_type))) {
    LOG_WARN("fail to build const double expr", K(ret), K(dval));
  }
  return ret;
}

int ObDSLResolver::resolve_const_to_int(ObIJsonBase &req_node, ObConstRawExpr *&const_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  ObJsonNodeType json_type = req_node.json_type();
  int64_t ival = 0;
  if (json_type == ObJsonNodeType::J_INT || json_type == ObJsonNodeType::J_UINT) {
    if (OB_FAIL(req_node.to_int(ival, true))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to convert json node to int", K(ret), K(json_type));
    }
  } else if (json_type == ObJsonNodeType::J_DOUBLE || json_type == ObJsonNodeType::J_STRING) {
    // cannot use to_int: it uses rint() (rounds to nearest) but DSL requires truncation
    double dval = 0.0;
    if (OB_FAIL(req_node.to_double(dval))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to convert to double for int target", K(ret), K(json_type));
    } else {
      LOG_TRACE("checking double-to-int64 overflow boundary", K(dval), K(json_type), "double_INT64_MAX", static_cast<double>(INT64_MAX));
      if (dval >= static_cast<double>(INT64_MAX) || dval < static_cast<double>(INT64_MIN)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("value overflows int64", K(ret), K(dval), K(json_type));
      } else {
        ival = static_cast<int64_t>(dval);
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unsupported json type to int", K(ret), K(json_type));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, ival, const_expr))) {
    LOG_WARN("fail to create const int expr", K(ret));
  }
  return ret;
}

int ObDSLResolver::resolve_const_to_uint(ObIJsonBase &req_node, ObConstRawExpr *&const_expr)
{
  int ret = OB_SUCCESS;
  // only for json uint
  if (req_node.json_type() != ObJsonNodeType::J_UINT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expects json uint", K(ret), K(req_node.json_type()));
  } else if (OB_FAIL(ObRawExprUtils::build_const_uint_expr(*params_.expr_factory_, ObUInt64Type, req_node.get_uint(), const_expr))) {
    LOG_WARN("fail to create const uint expr from json uint", K(ret));
  }
  return ret;
}

int ObDSLResolver::resolve_default_params(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  const ObString size_key = "size";
  const ObString from_key = "from";
  const ObString rank_key = "rank";
  const ObString min_score_key = "min_score";
  const ObString rerank_key = "rerank";
  ObIJsonBase *size_node = nullptr;
  ObIJsonBase *from_node = nullptr;
  ObIJsonBase *rank_node = nullptr;
  ObIJsonBase *min_score_node = nullptr;
  ObIJsonBase *rerank_node = nullptr;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  if (OB_FAIL(req_node.get_object_value(size_key, size_node))) {
    if (OB_SEARCH_NOT_FOUND == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get size node", K(ret));
    }
  } else if (OB_FAIL(resolve_size(*size_node))) {
    LOG_WARN("fail to resolve size", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(req_node.get_object_value(from_key, from_node))) {
    if (OB_SEARCH_NOT_FOUND == ret) {
      ret = OB_SUCCESS;
      ObConstRawExpr *from_expr = nullptr;
      if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, FROM_DEFAULT, from_expr))) {
        LOG_WARN("fail to create const expr for default from", K(ret));
      } else {
        dsl_query_info_->from_ = from_expr;
      }
    } else {
      LOG_WARN("fail to get from node", K(ret));
    }
  } else if (OB_FAIL(resolve_from(*from_node))) {
    LOG_WARN("fail to resolve from", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(req_node.get_object_value(min_score_key, min_score_node))) {
    if (OB_SEARCH_NOT_FOUND == ret) {
      ret = OB_SUCCESS;
      ObConstRawExpr *min_score_expr = nullptr;
      if (OB_FAIL(ObRawExprUtils::build_const_double_expr(*expr_factory, ObDoubleType, MIN_SCORE_DEFAULT, min_score_expr))) {
        LOG_WARN("fail to create const expr for default min score", K(ret));
      } else {
        dsl_query_info_->min_score_ = min_score_expr;
      }
    } else {
      LOG_WARN("fail to get min score node", K(ret));
    }
  } else if (OB_FAIL(resolve_min_score(*min_score_node))) {
    LOG_WARN("fail to resolve min score", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(req_node.get_object_value(rank_key, rank_node))) {
    if (OB_SEARCH_NOT_FOUND == ret) {
      ret = OB_SUCCESS;
      dsl_query_info_->rank_info_.method_ = ObFusionMethod::WEIGHT_SUM;
    } else {
      LOG_WARN("fail to get rank node", K(ret));
    }
  } else if (OB_FAIL(resolve_rank(*rank_node))) {
    LOG_WARN("fail to resolve rank", K(ret));
  } else {
    dsl_query_info_->rank_info_.has_rank_ = true;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(req_node.get_object_value(rerank_key, rerank_node))) {
    if (OB_SEARCH_NOT_FOUND == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get rerank node", K(ret));
    }
  } else if (OB_FAIL(resolve_rerank(*rerank_node))) {
    LOG_WARN("fail to resolve rerank", K(ret));
  }

  bool has_from_node = OB_NOT_NULL(from_node);
  bool has_rank_node = OB_NOT_NULL(rank_node);
  bool has_rerank_node = OB_NOT_NULL(rerank_node);
  bool has_size_node = OB_NOT_NULL(size_node);

  // 1) Set size and rank_window_size (defaults where not provided).
  // ES behavior:
  // - Only rank (no rerank): size = rank.rws
  // - Only rerank (no rank node): size = rerank.rws, rank.rws = rerank.rws
  // - Both (rank node + rerank): size = rerank.rws, rank.rws = 10 (fixed if not specified)
  // Key insight: size always inherits from rerank.rws when rerank exists
  if (OB_SUCC(ret)) {
    if (!has_rank_node && !has_rerank_node) {
      if (OB_FAIL(set_default_rank_window_size())) {
        LOG_WARN("fail to set default rank window size", K(ret));
      }
    } else if (!has_rank_node && has_rerank_node) {
      dsl_query_info_->rank_info_.window_size_ = dsl_query_info_->rank_info_.rerank_info_->rank_window_size_;
    }
    if (OB_SUCC(ret) && !has_size_node) {
      if (has_rerank_node) {
        dsl_query_info_->size_ = dsl_query_info_->rank_info_.rerank_info_->rank_window_size_;
      } else if (has_rank_node) {
        dsl_query_info_->size_ = dsl_query_info_->rank_info_.window_size_;
      }
    }
  }

  // 2) Get from/size/rank_window_size/rerank_window_size values and validate them.
  if (OB_SUCC(ret)) {
    int64_t from_value = 0;
    int64_t size_value = 0;
    int64_t rank_window_size_value = 0;
    int64_t rerank_window_size_value = 0;
    ObConstRawExpr *from_expr = static_cast<ObConstRawExpr *>(dsl_query_info_->from_);
    ObConstRawExpr *size_expr = static_cast<ObConstRawExpr *>(dsl_query_info_->size_);
    ObConstRawExpr *rank_window_size_expr = static_cast<ObConstRawExpr *>(dsl_query_info_->rank_info_.window_size_);

    if (OB_ISNULL(from_expr) || OB_ISNULL(size_expr) || OB_ISNULL(rank_window_size_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("from/size/rank_window_size expr should be set here", K(ret));
    } else if (OB_FAIL(from_expr->get_value().get_int(from_value))) {
      LOG_WARN("fail to get int value from from expr", K(ret));
    } else if (OB_FAIL(size_expr->get_value().get_int(size_value))) {
      LOG_WARN("fail to get int value from size expr", K(ret));
    } else if (OB_FAIL(rank_window_size_expr->get_value().get_int(rank_window_size_value))) {
      LOG_WARN("fail to get int value from rank_window_size expr", K(ret));
    } else if (has_rerank_node) {
      ObDSLRerankInfo *rerank_info = dsl_query_info_->rank_info_.rerank_info_;
      ObConstRawExpr *rerank_window_size_expr = static_cast<ObConstRawExpr *>(rerank_info->rank_window_size_);
      if (OB_FAIL(rerank_window_size_expr->get_value().get_int(rerank_window_size_value))) {
        LOG_WARN("fail to get int value from rerank rank_window_size expr", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (has_rank_node && has_rerank_node && has_size_node) {
        // size <= rerank_window_size <= rank_window_size
        if (size_value > rerank_window_size_value) {
          ret = OB_INVALID_CONFIG;
          LOG_USER_ERROR(OB_INVALID_CONFIG,
                         "hybrid search DSL: size must be <= rerank.rank_window_size");
          LOG_WARN("size is greater than rerank.rank_window_size", K(ret), K(size_value), K(rerank_window_size_value));
        } else if (rerank_window_size_value > rank_window_size_value) {
          ret = OB_INVALID_CONFIG;
          LOG_USER_ERROR(OB_INVALID_CONFIG,
                         "hybrid search DSL: rerank.rank_window_size must be <= rank.rank_window_size");
          LOG_WARN("rerank.rank_window_size is greater than rank.rank_window_size", K(ret), K(rerank_window_size_value), K(rank_window_size_value));
        }
      } else if (has_rank_node && has_rerank_node) {
        // rerank_window_size <= rank_window_size
        if (rerank_window_size_value > rank_window_size_value) {
          ret = OB_INVALID_CONFIG;
          LOG_USER_ERROR(OB_INVALID_CONFIG,
                         "hybrid search DSL: rerank.rank_window_size must be <= rank.rank_window_size");
          LOG_WARN("rerank.rank_window_size is greater than rank.rank_window_size", K(ret), K(rerank_window_size_value), K(rank_window_size_value));
        }
      } else if (has_rank_node && has_size_node) {
        // size <= rank_window_size
        if (size_value > rank_window_size_value) {
          ret = OB_INVALID_CONFIG;
          LOG_USER_ERROR(OB_INVALID_CONFIG,
                         "hybrid search DSL: size must be <= rank.rank_window_size");
          LOG_WARN("size is greater than rank.rank_window_size", K(ret), K(size_value), K(rank_window_size_value));
        }
      } else if (has_rerank_node && has_size_node) {
        // size <= rerank_window_size
        if (size_value > rerank_window_size_value) {
          ret = OB_INVALID_CONFIG;
          LOG_USER_ERROR(OB_INVALID_CONFIG,
                         "hybrid search DSL: size must be <= rerank.rank_window_size");
          LOG_WARN("size is greater than rerank.rank_window_size", K(ret), K(size_value), K(rerank_window_size_value));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (size_value + from_value > SIZE_VALUE_MAX) {
        ret = OB_INVALID_CONFIG;
        LOG_USER_ERROR(OB_INVALID_CONFIG,
                        "hybrid search DSL: from + size must be in range [0, 10000]");
        LOG_WARN("from+size out of range", K(ret), K(from_value), K(size_value), K(from_value + size_value));
      }
    }
  }
  return ret;
}

int ObDSLResolver::resolve_field(ObIJsonBase &field_node, ObColumnRefRawExpr *&col_expr, ObConstRawExpr *&boost_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  if (field_node.json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "field, should be string");
    LOG_WARN("field should be string", K(ret), K(field_node.json_type()));
  } else {
    const char caret = '^';
    ObString field_str = ObString(field_node.get_data_length(), field_node.get_data());
    ObString col_name = field_str.split_on(caret);
    char *boost_str = nullptr;
    double boost = 1.0;
    field_str = field_str.trim();
    if (col_name.empty()) {
      col_name = field_str;
    } else if (OB_UNLIKELY(field_str.empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("missing boost after caret", K(ret), K(col_name));
    } else if (OB_ISNULL(boost_str = static_cast<char *>(allocator_->alloc(field_str.length() + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(field_str.length()));
    } else {
      char *end_ptr = nullptr;
      memcpy(boost_str, field_str.ptr(), field_str.length());
      boost_str[field_str.length()] = '\0';
      boost = strtod(boost_str, &end_ptr);
      if (OB_UNLIKELY(end_ptr != boost_str + field_str.length())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("failed to parse boost", K(ret));
      } else if (OB_UNLIKELY(boost <= 0.0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "boost value after field, it must be a positive number");
        LOG_WARN("unexpected non-positive boost", K(ret), K(boost));
      }
    }
    if (FAILEDx(get_user_column_expr(col_name, col_expr))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, col_name.length(), col_name.ptr(),
                       table_item_.get_table_name().length(), table_item_.get_table_name().ptr());
      }
      LOG_WARN("fail to get user column expr", K(ret), K(col_name));
    } else if (boost == 1.0) {
      boost_expr = static_cast<ObConstRawExpr*>(dsl_query_info_->one_const_expr_);
    } else if (OB_FAIL(ObRawExprUtils::build_const_double_expr(*expr_factory, ObDoubleType, boost, boost_expr))) {
      LOG_WARN("fail to create boost expr", K(ret));
    }
  }
  return ret;
}

int ObDSLResolver::resolve_from(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  ObRawExpr *from_expr = nullptr;
  if (OB_FAIL(resolve_const(req_node, from_expr, ObJsonNodeType::J_INT))) {
    LOG_WARN("fail to resolve from value", K(ret));
  } else {
    int64_t from_value = 0;
    ObConstRawExpr *from_const = static_cast<ObConstRawExpr *>(from_expr);
    if (OB_FAIL(from_const->get_value().get_int(from_value))) {
      LOG_WARN("fail to get int value from from expr", K(ret));
    } else if (from_value < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "from, from value should be a non-negative integer");
      LOG_WARN("from value should be a non-negative integer", K(ret), K(from_value));
    } else if (from_value > SIZE_VALUE_MAX) {
      ret = OB_INVALID_CONFIG;
      LOG_USER_ERROR(OB_INVALID_CONFIG,
        "hybrid search DSL: from + size must be in range [0, 10000]");
      LOG_WARN("from value should be in range [0, 10000]", K(ret), K(from_value));
    }
  }
  if (OB_SUCC(ret)) {
    dsl_query_info_->from_ = from_expr;
  }
  return ret;
}

int ObDSLResolver::resolve_json_contains(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type)
{
  return resolve_json_expr(req_node, query, parent_query, outer_query_type, QUERY_ITEM_JSON_CONTAINS);
}

int ObDSLResolver::resolve_json_expr(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type, ObEsQueryItem query_type)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  ObItemType expr_type = T_MAX;
  ObString col_name;
  ObString path_str;
  ObString expr_name;
  ObIJsonBase *param_node = nullptr;
  ObColumnRefRawExpr *col_expr = nullptr;
  ObRawExpr *candidate_expr = nullptr;
  ObRawExpr *target_expr = nullptr;
  ObSysFunRawExpr *json_expr = nullptr;
  ObConstRawExpr *boost_expr = nullptr;
  ObDSLScalarQuery *json_query = nullptr;
  if (OB_FAIL(ret)) {
  } else if (query_type == QUERY_ITEM_JSON_CONTAINS) {
    expr_type = T_FUN_SYS_JSON_CONTAINS;
    expr_name = N_JSON_CONTAINS;
  } else if (query_type == QUERY_ITEM_JSON_MEMBER_OF) {
    expr_type = T_FUN_SYS_JSON_MEMBER_OF;
    expr_name = N_JSON_MEMBER_OF;
  } else if (query_type == QUERY_ITEM_JSON_OVERLAPS) {
    expr_type = T_FUN_SYS_JSON_OVERLAPS;
    expr_name = N_JSON_OVERLAPS;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported json query type", K(ret), K(query_type));
  }
  if (OB_FAIL(ret)) {
  } else if (ObDSLQuery::check_need_cal_score_in_bool(outer_query_type, parent_query)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "json query in must/should clause");
    LOG_WARN("json query cannot be scored or exist in must/should clause", K(ret), K(outer_query_type));
  } else if (req_node.json_type() != ObJsonNodeType::J_OBJECT || req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "json query, should be single-key object");
    LOG_WARN("json query should be single-key object", K(ret));
  } else if (OB_FAIL(req_node.get_object_value(0, col_name, param_node))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (param_node->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "json query param, should be object");
    LOG_WARN("json query param should be object", K(ret));
  } else {
    for (uint64_t i = 0; OB_SUCC(ret) && i < param_node->element_count(); i++) {
      ObString key;
      ObIJsonBase *sub_node = nullptr;
      if (OB_FAIL(param_node->get_object_value(i, key, sub_node))) {
        LOG_WARN("fail to get value", K(ret), K(i));
      } else if (key.case_compare("candidate") == 0) {
        ObJsonNodeType candidate_type = ObJsonNodeType::J_STRING;
        if (query_type == QUERY_ITEM_JSON_MEMBER_OF ||
            (query_type == QUERY_ITEM_JSON_CONTAINS &&
             sub_node->json_type() != ObJsonNodeType::J_STRING &&
             sub_node->json_type() != ObJsonNodeType::J_OBJECT &&
             sub_node->json_type() != ObJsonNodeType::J_ARRAY)) {
          candidate_type = resolve_type_mapping(sub_node->json_type());
        }
        if (OB_FAIL(resolve_const(*sub_node, candidate_expr, candidate_type, query_type))) {
          LOG_WARN("fail to resolve candidate", K(ret));
        }
      } else if (key.case_compare("path") == 0) {
        if (sub_node->json_type() != ObJsonNodeType::J_STRING) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("path should be string type", K(ret), K(sub_node->json_type()));
        } else if (OB_FALSE_IT(path_str.assign_ptr(sub_node->get_data(), sub_node->get_data_length()))) {
        }
      } else if (key.case_compare("boost") == 0) {
        if (OB_FAIL(resolve_boost(*sub_node, boost_expr, query_type, outer_query_type))) {
          LOG_WARN("fail to resolve boost", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "keys other than \"candidate\", \"path\" and \"boost\" in json query");
        LOG_WARN("unsupported key in json query", K(ret), K(key), K(query_type));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(candidate_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("candidate is required", K(ret));
  } else if (OB_FAIL(get_user_column_expr(col_name, col_expr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "json query, column not exists");
    }
    LOG_WARN("fail to get user column expr", K(ret), K(col_name));
  } else if (!col_expr->get_result_type().is_json()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "json expression, column is not a json type");
    LOG_WARN("json expression for non-json column", K(ret), K(col_name));
  } else if (OB_FAIL(build_field_expr_with_path(col_expr, path_str, query_type, target_expr))) {
    LOG_WARN("fail to build field expr with path for json query", K(ret));
  } else if (OB_FAIL(expr_factory->create_raw_expr(expr_type, json_expr))) {
    LOG_WARN("fail to create json expr", K(ret));
  } else if (OB_FALSE_IT(json_expr->set_func_name(expr_name))) {
  } else if (query_type == QUERY_ITEM_JSON_MEMBER_OF) {
    if (OB_FAIL(json_expr->set_param_exprs(candidate_expr, target_expr))) {
      LOG_WARN("fail to set param exprs", K(ret), K(query_type));
    }
  } else if (query_type == QUERY_ITEM_JSON_CONTAINS ||
             query_type == QUERY_ITEM_JSON_OVERLAPS) {
    if (OB_FAIL(json_expr->set_param_exprs(target_expr, candidate_expr))) {
      LOG_WARN("fail to set param exprs", K(ret), K(query_type));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDSLScalarQuery::create(*allocator_, json_query, query_type, outer_query_type, parent_query))) {
    LOG_WARN("fail to create json query", K(ret));
  } else {
    json_query->field_ = target_expr;
    json_query->scalar_expr_ = json_expr;
    json_query->boost_ = setup_boost(boost_expr);
    query = json_query;
  }
  return ret;
}

int ObDSLResolver::resolve_json_member_of(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type)
{
  return resolve_json_expr(req_node, query, parent_query, outer_query_type, QUERY_ITEM_JSON_MEMBER_OF);
}

int ObDSLResolver::resolve_json_overlaps(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type)
{
  return resolve_json_expr(req_node, query, parent_query, outer_query_type, QUERY_ITEM_JSON_OVERLAPS);
}

int ObDSLResolver::resolve_search_options(ObIJsonBase &req_node, ObDSLKnnQuery::SearchOption *&search_option)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  int64_t ef_search = 1000;
  double refine_k = 1.0;
  double drop_ratio_search = 0.0;
  int64_t ivf_nprobes = 1;
  int64_t bruteforce_fallback_threshold = 0;
  int64_t post_filter_max_scan_rows = 0;
  double pre_filter_threshold = 0.0;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "search_options, should be object");
    LOG_WARN("search_options should be object", K(ret));
  } else if (OB_ISNULL(search_option) &&
             OB_ISNULL(search_option = OB_NEWx(ObDSLKnnQuery::SearchOption, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for search option", K(ret));
  } else if (OB_FALSE_IT(count = req_node.element_count())) {
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObString key;
    ObIJsonBase *sub_node = nullptr;
    if (OB_FAIL(req_node.get_object_value(i, key, sub_node))) {
      LOG_WARN("fail to get value", K(ret), K(i));
    } else if (key.case_compare("ef_search") == 0) {
      if (search_option->param_.is_set_ef_search_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "duplicate field ef_search in search options");
        LOG_WARN("duplicate field ef_search in search options", K(ret));
      } else if (sub_node->json_type() != ObJsonNodeType::J_INT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("ef_search should be int type", K(ret), K(sub_node->json_type()));
      } else if (OB_FAIL(sub_node->to_int(ef_search))) {
        LOG_WARN("fail to get int value from ef_search", K(ret));
      } else if (ef_search < 1 || ef_search > 10000) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("ef_search value not in valid range", K(ret), K(ef_search));
      } else {
        search_option->param_.ef_search_ = ef_search;
        search_option->param_.is_set_ef_search_ = 1;
      }
    } else if (key.case_compare("refine_k") == 0) {
      ObJsonNodeType json_type = sub_node->json_type();
      if (!sub_node->is_json_number(json_type)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("refine_k should be number type", K(ret), K(json_type));
      } else if (OB_FAIL(sub_node->to_double(refine_k))) {
        LOG_WARN("fail to get double value from refine_k", K(ret));
      } else if (refine_k < 1.0 || refine_k > 1000.0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("refine_k value not in valid range", K(ret), K(refine_k));
      } else {
        search_option->param_.refine_k_ = refine_k;
        search_option->param_.is_set_refine_k_ = 1;
      }
    } else if (key.case_compare("filter_mode") == 0) {
      ObString filter_mode_str;
      if (sub_node->json_type() != ObJsonNodeType::J_STRING) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("filter_mode should be string type", K(ret), K(sub_node->json_type()));
      } else if (OB_FALSE_IT(filter_mode_str.assign_ptr(sub_node->get_data(), sub_node->get_data_length()))) {
      } else if (filter_mode_str.case_compare("pre") == 0) {
        search_option->filter_mode_ = ObKnnFilterMode::PRE_ADAPTIVE;
      } else if (filter_mode_str.case_compare("pre-knn") == 0) {
        search_option->filter_mode_ = ObKnnFilterMode::PRE_KNN;
      } else if (filter_mode_str.case_compare("pre-brute") == 0) {
        search_option->filter_mode_ = ObKnnFilterMode::PRE_BRUTE_FORCE;
      } else if (filter_mode_str.case_compare("post") == 0) {
        search_option->filter_mode_ = ObKnnFilterMode::POST_FILTER;
      } else if (filter_mode_str.case_compare("post-index-merge") == 0) {
        search_option->filter_mode_ = ObKnnFilterMode::POST_INDEX_MERGE;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid filter mode", K(ret), K(filter_mode_str));
      }
    } else if (key.case_compare("drop_ratio_search") == 0) {
      if (sub_node->json_type() != ObJsonNodeType::J_DOUBLE) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("drop_ratio_search should be double type", K(ret), K(sub_node->json_type()));
      } else if (OB_FAIL(sub_node->to_double(drop_ratio_search))) {
        LOG_WARN("fail to get double value from drop_ratio_search", K(ret));
      } else if (drop_ratio_search < 0.0 || drop_ratio_search > 0.9) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("drop_ratio_search value not in valid range", K(ret), K(drop_ratio_search));
      } else {
        search_option->param_.ob_sparse_drop_ratio_search_ = drop_ratio_search;
        search_option->param_.is_set_drop_ratio_search_ = 1;
      }
    } else if (key.case_compare("ivf_nprobes") == 0) {
      if (sub_node->json_type() != ObJsonNodeType::J_INT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("ivf_nprobes should be int type", K(ret), K(sub_node->json_type()));
      } else if (OB_FAIL(sub_node->to_int(ivf_nprobes))) {
        LOG_WARN("fail to get int value from ivf_nprobes", K(ret));
      } else if (ivf_nprobes < 1 || ivf_nprobes > 65536) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("ivf_nprobes value not in valid range", K(ret), K(ivf_nprobes));
      } else {
        search_option->param_.ivf_nprobes_ = ivf_nprobes;
        search_option->param_.is_set_ivf_nprobes_ = 1;
      }
    } else if (key.case_compare("primary_get_ratio") == 0) {
      if (OB_FAIL(resolve_primary_get_ratio(*sub_node, *search_option))) {
        LOG_WARN("fail to resolve primary_get_ratio", K(ret));
      }
    } else if (key.case_compare("bruteforce_fallback_threshold") == 0) {
      if (search_option->param_.is_set_bruteforce_fallback_threshold_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "duplicate field bruteforce_fallback_threshold in search options");
        LOG_WARN("duplicate field bruteforce_fallback_threshold in search options", K(ret));
      } else if (sub_node->json_type() != ObJsonNodeType::J_INT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("bruteforce_fallback_threshold should be int type", K(ret), K(sub_node->json_type()));
      } else if (OB_FAIL(sub_node->to_int(bruteforce_fallback_threshold))) {
        LOG_WARN("fail to get int value from bruteforce_fallback_threshold", K(ret));
      } else if (bruteforce_fallback_threshold < static_cast<int64_t>(ObVecIdxExtraInfo::MIN_BRUTEFORCE_FALLBACK_THRESHOLD)
                 || bruteforce_fallback_threshold > static_cast<int64_t>(ObVecIdxExtraInfo::MAX_BRUTEFORCE_FALLBACK_THRESHOLD)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("bruteforce_fallback_threshold value not in valid range", K(ret), K(bruteforce_fallback_threshold));
      } else {
        search_option->param_.bruteforce_fallback_threshold_ = static_cast<int32_t>(bruteforce_fallback_threshold);
        search_option->param_.is_set_bruteforce_fallback_threshold_ = 1;
      }
    } else if (key.case_compare("post_filter_max_scan_rows") == 0) {
      if (search_option->param_.is_set_post_filter_max_scan_rows_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "duplicate field post_filter_max_scan_rows in search options");
        LOG_WARN("duplicate field post_filter_max_scan_rows in search options", K(ret));
      } else if (sub_node->json_type() != ObJsonNodeType::J_INT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("post_filter_max_scan_rows should be int type", K(ret), K(sub_node->json_type()));
      } else if (OB_FAIL(sub_node->to_int(post_filter_max_scan_rows))) {
        LOG_WARN("fail to get int value from post_filter_max_scan_rows", K(ret));
      } else if (post_filter_max_scan_rows < 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("post_filter_max_scan_rows value not in valid range", K(ret), K(post_filter_max_scan_rows));
      } else {
        search_option->param_.post_filter_max_scan_rows_ = post_filter_max_scan_rows;
        search_option->param_.is_set_post_filter_max_scan_rows_ = 1;
      }
    } else if (key.case_compare("pre_filter_threshold") == 0) {
      if (search_option->param_.is_set_pre_filter_threshold_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "duplicate field pre_filter_threshold in search options");
        LOG_WARN("duplicate field pre_filter_threshold in search options", K(ret));
      } else if (!sub_node->is_json_number(sub_node->json_type())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("pre_filter_threshold should be number type", K(ret), K(sub_node->json_type()));
      } else if (OB_FAIL(sub_node->to_double(pre_filter_threshold))) {
        LOG_WARN("fail to get double value from pre_filter_threshold", K(ret));
      } else if (pre_filter_threshold < 0.0 || pre_filter_threshold > 1.0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("pre_filter_threshold value not in valid range", K(ret), K(pre_filter_threshold));
      } else {
        search_option->param_.pre_filter_threshold_ = static_cast<float>(pre_filter_threshold);
        search_option->param_.is_set_pre_filter_threshold_ = 1;
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported key in search_option", K(ret), K(key));
    }
  }
  return ret;
}

int ObDSLResolver::resolve_primary_get_ratio(ObIJsonBase &sub_node, ObDSLBaseSearchOption &base_option)
{
  int ret = OB_SUCCESS;
  const ObJsonNodeType node_type = sub_node.json_type();
  int64_t ratio = 0;
  if (node_type != ObJsonNodeType::J_INT && node_type != ObJsonNodeType::J_UINT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "primary_get_ratio, should be integer");
    LOG_WARN("primary_get_ratio should be integer", KR(ret), K(node_type));
  } else if (OB_FALSE_IT(ratio = sub_node.get_int())) {
  } else if (ratio < 1 || ratio > 1000000000) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "primary_get_ratio, should be in range [1, 1000000000]");
    LOG_WARN("primary_get_ratio should be in range [1, 1000000000]", KR(ret), K(ratio));
  } else {
    base_option.primary_get_ratio_ = ratio;
  }
  return ret;
}

int ObDSLResolver::dispatch_query_type(const ObString &key, ObIJsonBase &sub_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type)
{
  int ret = OB_SUCCESS;
  if (key.case_compare("bool") == 0) {
    if (OB_FAIL(resolve_bool(sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve bool", K(ret));
    }
  } else if (key.case_compare("range") == 0) {
    if (OB_FAIL(resolve_range(sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve range", K(ret));
    }
  } else if (key.case_compare("term") == 0) {
    if (OB_FAIL(resolve_term(sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve term", K(ret));
    }
  } else if (key.case_compare("wildcard") == 0) {
    if (OB_FAIL(resolve_wildcard(sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve wildcard", K(ret));
    }
  } else if (key.case_compare("terms") == 0) {
    if (OB_FAIL(resolve_terms(sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve terms", K(ret));
    }
  } else if (key.case_compare("match") == 0) {
    if (OB_FAIL(resolve_match(sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve match", K(ret));
    }
  } else if (key.case_compare("match_phrase") == 0) {
    if (OB_FAIL(resolve_match_phrase(sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve match phrase", K(ret));
    }
  } else if (key.case_compare("multi_match") == 0) {
    if (OB_FAIL(resolve_multi_match(sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve multi match", K(ret));
    }
  } else if (key.case_compare("query_string") == 0) {
    if (OB_FAIL(resolve_query_string(sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve query string", K(ret));
    }
  } else if (key.case_compare("array_contains") == 0) {
    if (OB_FAIL(resolve_array_contains(sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve array contains", K(ret));
    }
  } else if (key.case_compare("array_contains_all") == 0) {
    if (OB_FAIL(resolve_array_contains_all(sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve array contains_all", K(ret));
    }
  } else if (key.case_compare("array_overlaps") == 0) {
    if (OB_FAIL(resolve_array_overlaps(sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve array overlaps", K(ret));
    }
  } else if (key.case_compare("json_contains") == 0) {
    if (OB_FAIL(resolve_json_contains(sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve json contains", K(ret));
    }
  } else if (key.case_compare("json_member_of") == 0) {
    if (OB_FAIL(resolve_json_member_of(sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve json member_of", K(ret));
    }
  } else if (key.case_compare("json_overlaps") == 0) {
    if (OB_FAIL(resolve_json_overlaps(sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve json overlaps", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid key in query");
    LOG_WARN("not supported syntax in query", K(ret), K(key));
  }
  return ret;
}

int ObDSLResolver::resolve_single_term(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type)
{
  int ret = OB_SUCCESS;
  ObString key;
  ObIJsonBase *sub_node = nullptr;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT || req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query item, should be single-key object");
    LOG_WARN("query item should be single-key object", K(ret), K(req_node.json_type()), K(req_node.element_count()));
  } else if (OB_FAIL(req_node.get_object_value(0, key, sub_node))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (OB_FAIL(dispatch_query_type(key, *sub_node, query, parent_query, outer_query_type))) {
    LOG_WARN("fail to dispatch query type", K(ret), K(key));
  }
  return ret;
}

int ObDSLResolver::resolve_size(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  ObRawExpr *size_expr = nullptr;
  if (OB_FAIL(resolve_const(req_node, size_expr, ObJsonNodeType::J_INT))) {
    LOG_WARN("fail to resolve size value", K(ret));
  } else {
    int64_t size_value = 0;
    ObConstRawExpr *size_const = static_cast<ObConstRawExpr *>(size_expr);
    if (OB_FAIL(size_const->get_value().get_int(size_value))) {
      LOG_WARN("fail to get int value from size expr", K(ret));
    } else if (size_value < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "size, size value should be a non-negative integer");
      LOG_WARN("size value should be a non-negative integer", K(ret), K(size_value));
    } else if (size_value > SIZE_VALUE_MAX) {
      ret = OB_INVALID_CONFIG;
      LOG_USER_ERROR(OB_INVALID_CONFIG,
                     "hybrid search DSL: from + size must be in range [0, 10000]");
      LOG_WARN("size value should be in range [0, 10000]", K(ret), K(size_value));
    }
  }
  if (OB_SUCC(ret)) {
    dsl_query_info_->size_ = size_expr;
  }
  return ret;
}

int ObDSLResolver::resolve_slop(ObIJsonBase &req_node, int32_t &slop)
{
  int ret = OB_SUCCESS;
  double slop_double = 0.0;
  ObJsonNodeType json_type = req_node.json_type();
  if (!ObIJsonBase::is_json_number_type(json_type) && json_type != ObJsonNodeType::J_STRING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "slop, should be number or string type");
    LOG_WARN("slop should be number or string type", K(ret), K(json_type));
  } else if (OB_FAIL(req_node.to_double(slop_double))) {
    LOG_WARN("fail to get double value from slop", K(ret));
  } else if (slop_double < 0.0 || slop_double != static_cast<int64_t>(slop_double) || slop_double > INT32_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "slop, should be a non-negative integer and less than INT32_MAX");
    LOG_WARN("slop must be a non-negative integer and less than INT32_MAX", K(ret), K(slop_double));
  } else {
    slop = static_cast<int32_t>(slop_double);
  }
  return ret;
}

int ObDSLResolver::resolve_min_score(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  ObRawExpr *min_score_expr = nullptr;
  if (OB_FAIL(resolve_const(req_node, min_score_expr, ObJsonNodeType::J_DOUBLE))) {
    LOG_WARN("fail to resolve min score value", K(ret));
  } else {
    dsl_query_info_->min_score_ = min_score_expr;
  }
  return ret;
}

int ObDSLResolver::dsl_bool_msm_snapshot_from_msm_expr(ObConstRawExpr *msm_expr, int32_t &msm_snapshot)
{
  int ret = OB_SUCCESS;
  static const int64_t INT32_MAX_AS_I64 = static_cast<int64_t>(2147483647LL);
  msm_snapshot = 1;
  if (msm_expr != nullptr) {
    const ObObj &v = msm_expr->get_value();
    const ObObjType tp = v.get_type();
    if (ob_is_string_type(tp)) {
      msm_snapshot = 1;
    } else if (ob_is_integer_type(tp)) {
      const int64_t raw = v.get_int();
      if (raw < 0) {
        msm_snapshot = 0;
      } else if (raw > INT32_MAX_AS_I64) {
        msm_snapshot = static_cast<int32_t>(INT32_MAX_AS_I64);
      } else {
        msm_snapshot = static_cast<int32_t>(raw);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected minimum_should_match const expr type", K(ret), K(tp));
    }
  }
  return ret;
}

int ObDSLResolver::resolve_minimum_should_match_expr(
    ObIJsonBase *sub_node,
    ObCollationType collation_type,
    const int32_t default_msm_i32,
    ObConstRawExpr *&msm_expr)
{
  int ret = OB_SUCCESS;
  msm_expr = nullptr;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  if (OB_ISNULL(sub_node)) {
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, default_msm_i32, msm_expr))) {
      LOG_WARN("failed to build default minimum_should_match expr", K(ret));
    }
  } else {
    ObCollationType coll = collation_type;
    if (CS_TYPE_INVALID == coll && OB_FAIL(session_info_->get_collation_connection(coll))) {
      LOG_WARN("failed to get collation_connection", K(ret));
    } else {
      const ObJsonNodeType msm_json_node_type = sub_node->json_type();
      if (ObJsonNodeType::J_INT == msm_json_node_type) {
        const int64_t msm_i64 = sub_node->get_int();
        if (msm_i64 < static_cast<int64_t>(INT32_MIN) || msm_i64 > static_cast<int64_t>(INT32_MAX)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "minimum_should_match, should be in range [-2147483648, 2147483647]");
          LOG_WARN("minimum_should_match out of int32 range", KR(ret), K(msm_i64));
        } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
                           *expr_factory, ObIntType, msm_i64, msm_expr))) {
          LOG_WARN("failed to build msm int expr", KR(ret));
        }
      } else if (ObJsonNodeType::J_STRING == msm_json_node_type) {
        const ObString raw(sub_node->get_data_length(), sub_node->get_data());
        const ObString trimmed = raw.trim();
        if (trimmed.empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "minimum_should_match string should not be empty");
          LOG_WARN("minimum_should_match empty string", KR(ret));
        } else {
          bool valid_int = false;
          const char *const p = trimmed.ptr();
          const char *const e = trimmed.ptr() + trimmed.length();
          const int32_t parsed = common::ObFastAtoi<int32_t>::atoi(p, e, valid_int);
          if (valid_int) {
            if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
                        *expr_factory, ObIntType, static_cast<int64_t>(parsed), msm_expr))) {
              LOG_WARN("failed to build msm int expr", KR(ret));
            }
          } else if (OB_FAIL(ObRawExprUtils::build_const_string_expr(
                             *expr_factory, ObVarcharType, trimmed, coll, msm_expr))) {
            LOG_WARN("failed to build msm string expr", KR(ret));
          }
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "minimum_should_match in match query");
        LOG_WARN("minimum_should_match unsupported json type", KR(ret), K(msm_json_node_type));
      }
    }
  }
  return ret;
}

int ObDSLResolver::resolve_multi_knn(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  ObJsonNodeType json_type = req_node.json_type();
  if (json_type != ObJsonNodeType::J_OBJECT && json_type != ObJsonNodeType::J_ARRAY) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "knn, should be object or array");
    LOG_WARN("knn should be object or array", K(ret), K(json_type));
  } else {
    uint64_t knn_count = json_type == ObJsonNodeType::J_OBJECT ? 1 : req_node.element_count();
    for (uint64_t i = 0; OB_SUCC(ret) && i < knn_count; i++) {
      ObIJsonBase *val_node = nullptr;
      ObDSLQuery *knn_query = nullptr;
      if (json_type == ObJsonNodeType::J_OBJECT) {
        val_node = &req_node;
      } else if (OB_FAIL(req_node.get_array_element(i, val_node))) {
        LOG_WARN("fail to get array element", K(ret), K(i));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(resolve_knn(*val_node, knn_query))) {
        LOG_WARN("fail to resolve knn", K(ret), K(i));
      } else if (OB_FAIL(dsl_query_info_->queries_.push_back(knn_query))) {
        LOG_WARN("fail to push back knn query", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObDSLResolver::resolve_multi_match(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type)
{
  int ret = OB_SUCCESS;
  const char *params_name[] = {"fields", "query"};
  RequiredParamsSet required_params;
  ObDSLMultiMatchQuery *multi_match_query = nullptr;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  hash::ObHashSet<int32_t> resolved_field_idx_set;
  if (OB_FAIL(ObDSLMultiMatchQuery::create(outer_query_type, parent_query, *allocator_, multi_match_query))) {
    LOG_WARN("fail to create multi match query", K(ret));
  } else if (OB_FAIL(resolved_field_idx_set.create(req_node.element_count()))) {
    LOG_WARN("fail to create resolved field idx set", K(ret));
  } else if (OB_UNLIKELY(req_node.json_type() != ObJsonNodeType::J_OBJECT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "multi_match query, should be object");
    LOG_WARN("multi_match query should be object", K(ret), K(req_node.json_type()));
  } else if (OB_FAIL(construct_required_params(params_name, sizeof(params_name)/sizeof(params_name[0]), required_params))) {
    LOG_WARN("fail to create required params set", K(ret));
  } else if (OB_FAIL(resolve_multi_fields_query_param(
    req_node, true, multi_match_query->fields_param_, required_params, resolved_field_idx_set))) {
    LOG_WARN("fail to resolve multi fields query param", K(ret));
  }

  ObConstRawExpr *query_expr = nullptr;
  ObConstRawExpr *boost_expr = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < req_node.element_count(); i++) {
    ObString key;
    ObIJsonBase *sub_node = nullptr;
    if (OB_FAIL(resolved_field_idx_set.exist_refactored(i))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
        continue;
      } else if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to check field idx exist", K(ret), K(i));
      }
    }

    if (FAILEDx(req_node.get_object_value(i, key, sub_node))) {
      LOG_WARN("fail to get value", K(ret), K(i));
    } else if (key.case_compare("query") == 0) {
      ObCollationType collation_type = multi_match_query->fields_param_.fields_.at(0)->get_collation_type();
      if (OB_FAIL(resolve_query_string_query(*sub_node, query_expr, collation_type))) {
        LOG_WARN("fail to resolve query_string query", K(ret));
      } else if (OB_FAIL(required_params.erase_refactored(ObString("query")))) {
        LOG_WARN("fail to erase set", K(ret));
      }
    } else if (key.case_compare("boost") == 0) {
      if (OB_FAIL(resolve_boost(*sub_node, boost_expr, QUERY_ITEM_MULTI_MATCH, outer_query_type))) {
        LOG_WARN("fail to resolve boost", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Parameter in multi_match query");
      LOG_WARN("not supported key in multi match query", K(ret), K(key));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!required_params.empty())) {
    ret = OB_INVALID_ARGUMENT;
    ObString param_name = required_params.begin()->first;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "multi_match, \"fields\" and \"query\" are required");
    LOG_WARN("multi_match required params are missing", K(ret), K(param_name));
  } else {
    multi_match_query->query_ = query_expr;
    multi_match_query->boost_ = setup_boost(boost_expr);
    query = multi_match_query;
  }
  return ret;
}

int ObDSLResolver::resolve_multi_fields_query_param(
    ObIJsonBase &req_node,
    const bool is_multi_match,
    ObDSLFullTextMultiFieldQueryParam &fields_param,
    RequiredParamsSet &required_params,
    hash::ObHashSet<int32_t> &resolved_field_idx_set)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColumnRefRawExpr*, 4, ModulePageAllocator, true> fields;
  ObSEArray<ObConstRawExpr*, 4, ModulePageAllocator, true> field_boosts;
  ObMatchFieldsType type = ObDSLFullTextMultiFieldQueryParam::DEFAULT_FIELD_TYPE;
  ObMatchOperator opr = ObDSLFullTextQuery::DEFAULT_OPERATOR;
  bool has_msm_key = false;
  ObIJsonBase *multi_msm_json_node = nullptr;
  ObConstRawExpr *minimum_should_match_expr = nullptr;
  ObConstRawExpr *operator_expr = nullptr;
  ObConstRawExpr *type_expr = nullptr;
  for (uint64_t i = 0; OB_SUCC(ret) && i < req_node.element_count(); i++) {
    ObString key;
    ObIJsonBase *sub_node = nullptr;
    bool key_matched = true;
    if (OB_FAIL(req_node.get_object_value(i, key, sub_node))) {
      LOG_WARN("fail to get value", K(ret), K(i));
    } else if (key.case_compare("fields") == 0) {
      bool compatible = true;
      if (OB_FAIL(resolve_query_string_fields(*sub_node, fields, field_boosts, compatible))) {
        if (OB_ERR_BAD_FIELD_ERROR != ret &&
            OB_ERR_FT_COLUMN_NOT_INDEXED != ret) {
          ret = OB_INVALID_ARGUMENT;
          // if not compatible, user error message is logged already, do not override it
          if (compatible) {
            LOG_USER_ERROR(OB_INVALID_ARGUMENT,
              is_multi_match ? "fields in multi_match query" : "fields in query_string query");
          }
        }
        LOG_WARN("fail to resolve query_string fields", K(ret));
      } else if (OB_FAIL(required_params.erase_refactored(ObString("fields")))) {
        LOG_WARN("fail to erase set", K(ret));
      }
    } else if (key.case_compare("type") == 0) {
      if (OB_FAIL(resolve_query_string_type(*sub_node, type))) {
        if (OB_NOT_SUPPORTED == ret) {
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
              is_multi_match ? "Type in multi_match query" : "Type in query_string query");
        } else if (OB_INVALID_ARGUMENT == ret) {
          LOG_USER_ERROR(OB_INVALID_ARGUMENT,
              is_multi_match ? "type in multi_match query" : "type in query_string query");
        }
        LOG_WARN("fail to resolve query_string type", K(ret));
      }
    } else if (is_multi_match && key.case_compare("operator") == 0) {
      if (OB_FAIL(resolve_query_string_operator(*sub_node, opr))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "operator in multi_match query, should be \"or\" or \"and\"");
        LOG_WARN("fail to resolve query_string operator", K(ret));
      }
    } else if (!is_multi_match && key.case_compare("default_operator") == 0) {
      if (OB_FAIL(resolve_query_string_operator(*sub_node, opr))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "default_operator in query_string query, should be \"or\" or \"and\"");
        LOG_WARN("fail to resolve query_string operator", K(ret));
      }
    } else if (key.case_compare("minimum_should_match") == 0) {
      has_msm_key = true;
      multi_msm_json_node = sub_node;
    } else {
      key_matched = false;
    }

    if (OB_FAIL(ret) || !key_matched) {
    } else if (OB_FAIL(resolved_field_idx_set.set_refactored(i))) {
      LOG_WARN("fail to set resolved field idx", K(ret), K(i));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (fields.empty()) {
    // in case the key "fields" is not found
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT,
      is_multi_match ? "multi_match, \"fields\" is necessary" :
                       "query_string, \"fields\" is necessary");
    LOG_WARN("fields is empty or not found", K(ret));
  } else if (OB_FAIL(fields_param.fields_.assign(fields))) {
    LOG_WARN("fail to assign fields", K(ret));
  } else if (OB_FAIL(fields_param.field_boosts_.assign(field_boosts))) {
    LOG_WARN("fail to assign field boosts", K(ret));
  } else if (OB_FAIL(resolve_minimum_should_match_expr(
                     has_msm_key ? multi_msm_json_node : nullptr,
                     CS_TYPE_INVALID,
                     ObDSLFullTextQuery::DEFAULT_MINIMUM_SHOULD_MATCH,
                     minimum_should_match_expr))) {
    LOG_WARN("fail to resolve or default minimum_should_match", K(ret));
  } else if (OB_ISNULL(minimum_should_match_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("minimum should match expr is null", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
      *params_.expr_factory_, ObIntType, opr, operator_expr))) {
    LOG_WARN("fail to build const int expr for operator", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
      *params_.expr_factory_, ObIntType, type, type_expr))) {
    LOG_WARN("fail to build const int expr for type", K(ret));
  } else {
    fields_param.minimum_should_match_ = minimum_should_match_expr;
    fields_param.operator_ = operator_expr;
    fields_param.field_type_ = type_expr;
  }
  return ret;
}

int ObDSLResolver::resolve_query(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  ObDSLQuery *fts_query = nullptr;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT || req_node.element_count() < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query, should be non-empty object");
    LOG_WARN("query node must be a non-empty JSON object", KR(ret));
  } else {
    bool found_query_type = false;
    for (uint64_t i = 0; OB_SUCC(ret) && i < req_node.element_count(); ++i) {
      ObString key;
      ObIJsonBase *sub_node = nullptr;
      if (OB_FAIL(req_node.get_object_value(i, key, sub_node))) {
        LOG_WARN("fail to get query value", KR(ret), K(i));
      } else if (key.case_compare("search_options") == 0) {
        if (OB_FAIL(resolve_query_search_options(*sub_node))) {
          LOG_WARN("failed to resolve query search_options", KR(ret));
        }
      } else if (found_query_type) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("query node must contain exactly one query type key", KR(ret), K(key));
      } else if (OB_FAIL(dispatch_query_type(key, *sub_node, fts_query, nullptr, QUERY_ITEM_QUERY))) {
        LOG_WARN("failed to dispatch query type", KR(ret), K(key));
      } else {
        found_query_type = true;
      }
    }
    if (OB_SUCC(ret) && !found_query_type) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("no query type key found in query node", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(setup_top_level_score(fts_query))) {
    LOG_WARN("fail to setup top level score", KR(ret));
  } else {
    dsl_query_info_->query_top_level_boost_ = fts_query->boost_;
    fts_query->boost_ = static_cast<ObConstRawExpr *>(dsl_query_info_->one_const_expr_);
    if (OB_FAIL(try_push_nested_boost_to_leaf_query(fts_query, 1.0))) {
      LOG_WARN("fail to push nested boost to leaf query", KR(ret));
    } else if (OB_FAIL(dsl_query_info_->queries_.push_back(fts_query))) {
      LOG_WARN("fail to push back fts query", KR(ret));
    }
  }
  return ret;
}

int ObDSLResolver::resolve_query_search_options(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query search_options, should be object");
    LOG_WARN("query search_options must be a JSON object", KR(ret));
  } else {
    for (uint64_t i = 0; OB_SUCC(ret) && i < req_node.element_count(); ++i) {
      ObString key;
      ObIJsonBase *sub_node = nullptr;
      if (OB_FAIL(req_node.get_object_value(i, key, sub_node))) {
        LOG_WARN("fail to get search_options value", KR(ret), K(i));
      } else if (key.case_compare("query_dop") == 0) {
        const ObJsonNodeType node_type = sub_node->json_type();
        int64_t dop = -1;
        if (node_type != ObJsonNodeType::J_INT &&
            node_type != ObJsonNodeType::J_UINT) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query_dop, should be integer");
          LOG_WARN("query_dop should be integer", KR(ret));
        } else if (OB_FALSE_IT(dop = sub_node->get_int())) {
        } else if (dop < 1 || dop > 128) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query_dop, should be in range [1, 128]");
          LOG_WARN("query_dop should be in range [1, 128]", KR(ret), K(dop));
        } else {
          dsl_query_info_->query_dop_ = static_cast<int32_t>(dop);
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "keys other than \"query_dop\" in search_options");
        LOG_WARN("unsupported key in search_options", KR(ret), K(key));
      }
    }
  }
  return ret;
}

int ObDSLResolver::resolve_query_string(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type)
{
  int ret = OB_SUCCESS;
  const char *params_name[] = {"fields", "query"};
  RequiredParamsSet required_params;
  ObDSLQueryStringQuery *query_string_query = nullptr;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  hash::ObHashSet<int32_t> resolved_field_idx_set;
  if (OB_FAIL(ObDSLQueryStringQuery::create(outer_query_type, parent_query, *allocator_, query_string_query))) {
    LOG_WARN("fail to create query string query", K(ret));
  } else if (OB_FAIL(resolved_field_idx_set.create(req_node.element_count()))) {
    LOG_WARN("fail to create resolved field idx set", K(ret));
  } else if (OB_UNLIKELY(req_node.json_type() != ObJsonNodeType::J_OBJECT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query_string query, should be object");
    LOG_WARN("query_string query should be object", K(ret), K(req_node.json_type()));
  } else if (OB_FAIL(construct_required_params(params_name, sizeof(params_name)/sizeof(params_name[0]), required_params))) {
    LOG_WARN("fail to create required params set", K(ret));
  } else if (OB_FAIL(resolve_multi_fields_query_param(
    req_node, false, query_string_query->fields_param_, required_params, resolved_field_idx_set))) {
    LOG_WARN("fail to resolve multi fields query param", K(ret));
  }

  ObConstRawExpr *query_expr = nullptr;
  ObConstRawExpr *boost_expr = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < req_node.element_count(); i++) {
    ObString key;
    ObIJsonBase *sub_node = nullptr;
    if (OB_FAIL(resolved_field_idx_set.exist_refactored(i))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
        continue;
      } else if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to check field idx exist", K(ret), K(i));
      }
    }

    if (FAILEDx(req_node.get_object_value(i, key, sub_node))) {
      LOG_WARN("fail to get value", K(ret), K(i));
    } else if (key.case_compare("query") == 0) {
      ObCollationType collation_type = query_string_query->fields_param_.fields_.at(0)->get_collation_type();
      if (OB_FAIL(resolve_query_string_query(*sub_node, query_expr, collation_type))) {
        LOG_WARN("fail to resolve query_string query", K(ret));
      } else if (OB_FAIL(required_params.erase_refactored(ObString("query")))) {
        LOG_WARN("fail to erase set", K(ret));
      }
    } else if (key.case_compare("boost") == 0) {
      if (OB_FAIL(resolve_boost(*sub_node, boost_expr, QUERY_ITEM_QUERY_STRING, outer_query_type))) {
        LOG_WARN("fail to resolve boost", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Parameter in query_string query");
      LOG_WARN("not supported key in query string query", K(ret), K(key));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!required_params.empty())) {
    ret = OB_INVALID_ARGUMENT;
    ObString param_name = required_params.begin()->first;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "query_string, \"fields\" and \"query\" are required");
    LOG_WARN("query_string required params are missing", K(ret), K(param_name));
  } else {
    query_string_query->query_ = query_expr;
    query_string_query->boost_ = setup_boost(boost_expr);
    query = query_string_query;
  }
  return ret;
}

int ObDSLResolver::resolve_query_string_fields(ObIJsonBase &req_node,
                                               ObIArray<ObColumnRefRawExpr*> &fields,
                                               ObIArray<ObConstRawExpr*> &field_boosts,
                                               bool &compatible)
{
  int ret = OB_SUCCESS;
  if (req_node.json_type() == ObJsonNodeType::J_ARRAY) {
    if (req_node.element_count() == 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "fields, should not be empty array");
      LOG_WARN("fields should not be empty", K(ret));
    }
    for (uint64_t i = 0; OB_SUCC(ret) && i < req_node.element_count(); i++) {
      ObIJsonBase *field_node = nullptr;
      ObColumnRefRawExpr *col_expr = nullptr;
      ObConstRawExpr *col_boost_expr = nullptr;
      int64_t col_idx = -1;
      if (OB_FAIL(req_node.get_array_element(i, field_node))) {
        LOG_WARN("fail to get field element", K(ret), K(i));
      } else if (OB_FAIL(resolve_field(*field_node, col_expr, col_boost_expr))) {
        LOG_WARN("fail to resolve field", K(ret), K(i));
      }
      for (int64_t j = 0; OB_SUCC(ret) && -1 == col_idx && j < fields.count(); j++) {
        if (fields.at(j) == col_expr) {
          col_idx = j;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (col_idx > -1) {
        field_boosts.at(col_idx) = col_boost_expr;
      } else if (OB_FAIL(fields.push_back(col_expr))) {
        LOG_WARN("fail to push back column expr", K(ret));
      } else if (OB_FAIL(field_boosts.push_back(col_boost_expr))) {
        LOG_WARN("fail to push back boost expr", K(ret));
      }
    }
  } else if (req_node.json_type() == ObJsonNodeType::J_STRING) {
    ObColumnRefRawExpr *col_expr = nullptr;
    ObConstRawExpr *col_boost_expr = nullptr;
    if (OB_FAIL(resolve_field(req_node, col_expr, col_boost_expr))) {
      LOG_WARN("fail to resolve field", K(ret));
    } else if (OB_FAIL(fields.push_back(col_expr))) {
      LOG_WARN("fail to push back column expr", K(ret));
    } else if (OB_FAIL(field_boosts.push_back(col_boost_expr))) {
      LOG_WARN("fail to push back boost expr", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "fields, should be string or array");
    LOG_WARN("fields should be string or array", K(ret), K(req_node.json_type()));
  }

  compatible = true;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_fields_collation_types(fields, compatible))) {
    LOG_WARN("fail to check fields collation types", K(ret));
  } else if (!compatible) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "fields, containing different collation types");
    LOG_WARN("fields have incompatible collation", K(ret));
  } else if (OB_FAIL(check_fields_parsers(fields, compatible))) {
    LOG_WARN("fail to check fields parsers", K(ret));
  } else if (!compatible) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "fields, indexed with different parsers");
    LOG_WARN("fields have incompatible parsers", K(ret));
  }
  return ret;
}

int ObDSLResolver::resolve_query_string_operator(ObIJsonBase &req_node, ObMatchOperator &opr)
{
  int ret = OB_SUCCESS;
  ObString opr_str;
  opr = MATCH_OPERATOR_OR;
  if (req_node.json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("operator should be string", K(ret), K(req_node.json_type()));
  } else if (OB_FALSE_IT(opr_str = ObString(req_node.get_data_length(), req_node.get_data()))) {
  } else if (opr_str.case_compare("or") == 0) {
    opr = MATCH_OPERATOR_OR;
  } else if (opr_str.case_compare("and") == 0) {
    opr = MATCH_OPERATOR_AND;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unsupported operator in fulltext query", K(ret), K(opr_str));
  }
  return ret;
}

int ObDSLResolver::resolve_query_string_query(ObIJsonBase &req_node, ObConstRawExpr *&query_expr, ObCollationType collation_type)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = nullptr;
  if (req_node.json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "multi_match.query or query_string.query, should be string");
    LOG_WARN("multi_match.query or query_string.query should be string", K(ret), K(req_node.json_type()));
  } else if (OB_FAIL(resolve_query_string_expr(ObString(req_node.get_data_length(), req_node.get_data()), collation_type, expr))) {
    LOG_WARN("fail to construct string expr", K(ret));
  } else {
    query_expr = static_cast<ObConstRawExpr*>(expr);
  }
  return ret;
}

int ObDSLResolver::resolve_query_string_type(ObIJsonBase &req_node, ObMatchFieldsType &type)
{
  int ret = OB_SUCCESS;
  ObString type_str;
  type = MATCH_BEST_FIELDS;
  if (req_node.json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "multi_match.type or query_string.type, should be string");
    LOG_WARN("multi_match.type or query_string.type should be string", K(ret), K(req_node.json_type()));
  } else if (OB_FALSE_IT(type_str = ObString(req_node.get_data_length(), req_node.get_data()))) {
  } else if (type_str.case_compare("best_fields") == 0) {
    type = MATCH_BEST_FIELDS;
  } else if (type_str.case_compare("most_fields") == 0) {
    type = MATCH_MOST_FIELDS;
  } else if (type_str.case_compare("cross_fields") == 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cross_fields type is not supported yet", K(ret));
    // TODO: type = MATCH_CROSS_FIELDS;
  } else if (type_str.case_compare("phrase") == 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("phrase type is not supported yet", K(ret));
    // TODO: type = MATCH_PHRASE;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "multi_match.type or query_string.type, should be \"best_fields\" or \"most_fields\"");
    LOG_WARN("unsupported query_string type", K(ret), K(type_str));
  }
  return ret;
}

int ObDSLResolver::resolve_range(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  uint64_t count = 0;
  ObString col_name;
  ObString path_str;
  ObIJsonBase *sub_node = nullptr;
  ObColumnRefRawExpr *col_expr = nullptr;
  ObRawExpr *field_expr = nullptr;
  ObRawExpr *scalar_expr = nullptr;
  ObSEArray<ObRawExpr*, 4, ModulePageAllocator, true> condition_exprs;
  ObConstRawExpr *boost_expr = nullptr;
  ObDSLScalarQuery *range_query = nullptr;
  if (ObDSLQuery::check_need_cal_score_in_bool(outer_query_type, parent_query)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "scalar range query in must/should clause");
    LOG_WARN("scalar range query cannot be scored or exist in must/should clause", K(ret), K(outer_query_type));
  } else if (req_node.json_type() != ObJsonNodeType::J_OBJECT || req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "range, should be single-key object");
    LOG_WARN("range query should be single-key object", K(ret));
  } else if (OB_FAIL(req_node.get_object_value(0, col_name, sub_node))) {
    LOG_WARN("fail to get object key and value", K(ret));
  } else if (sub_node->json_type() != ObJsonNodeType::J_OBJECT || sub_node->element_count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "range query, should be an object with at least one of \"gt\", \"gte\", \"lt\", \"lte\"");
    LOG_WARN("unexpected range condition", K(ret));
  } else if (OB_FALSE_IT(count = sub_node->element_count())) {
  } else if (OB_FAIL(get_field_expr_and_path(col_name, col_expr, path_str))) {
    LOG_WARN("fail to get field expr and path", K(ret), K(col_name));
  } else if (OB_FAIL(build_field_expr_with_path(col_expr, path_str, QUERY_ITEM_RANGE, field_expr))) {
    LOG_WARN("fail to build field expr with path for range", K(ret));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    ObString key;
    ObJsonNodeType value_type = ObJsonNodeType::J_MAX_TYPE;
    ObIJsonBase *var_node = nullptr;
    ObRawExpr *var_expr = nullptr;
    ObOpRawExpr *cmp_expr = nullptr;
    ObItemType type = T_INVALID;
    if (OB_FAIL(sub_node->get_object_value(i, key, var_node))) {
      LOG_WARN("fail to get value", K(ret), K(i));
    } else if (key.case_compare("gt") == 0) {
      type = T_OP_GT;
    } else if (key.case_compare("gte") == 0) {
      type = T_OP_GE;
    } else if (key.case_compare("lt") == 0) {
      type = T_OP_LT;
    } else if (key.case_compare("lte") == 0) {
      type = T_OP_LE;
    } else if (key.case_compare("boost") == 0) {
      if (OB_FAIL(resolve_boost(*var_node, boost_expr, QUERY_ITEM_RANGE, outer_query_type))) {
        LOG_WARN("fail to resolve boost", K(ret));
      } else {
        continue;
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "keys other than \"gt\", \"gte\", \"lt\", \"lte\" and \"boost\" in range query");
      LOG_WARN("unsupported key in range query", K(ret), K(key));
    }
    if (OB_FAIL(ret)) {
    } else if (!IS_RANGE_CMP_OP(type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid type", K(ret), K(type));
    } else if (!is_scalar_json_type(var_node->json_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "value in range query, should be string or number");
      LOG_WARN("invalid value type in range query", K(ret), K(var_node->json_type()));
    } else if (OB_FALSE_IT(value_type = resolve_type_mapping(var_node->json_type(), col_expr))) {
    } else if (OB_FAIL(resolve_const(*var_node, var_expr, value_type))) {
      LOG_WARN("fail to resolve const value", K(ret), K(i));
    } else if (OB_FAIL(expr_factory->create_raw_expr(type, cmp_expr))) {
      LOG_WARN("fail to create cmp expr", K(ret), K(type));
    } else if (OB_FAIL(cmp_expr->set_param_exprs(field_expr, var_expr))) {
      LOG_WARN("fail to set param exprs", K(ret), K(type));
    } else if (OB_FAIL(condition_exprs.push_back(cmp_expr))) {
      LOG_WARN("fail to add condition to array", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (condition_exprs.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "range query, should contain at least one of \"gt\", \"gte\", \"lt\", \"lte\"");
    LOG_WARN("range query has no comparison condition", K(ret));
  } else if (OB_FAIL(ObDSLScalarQuery::create(*allocator_, range_query, QUERY_ITEM_RANGE, outer_query_type, parent_query))) {
    LOG_WARN("fail to create range query", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_and_expr(*expr_factory, condition_exprs, scalar_expr))) {
    LOG_WARN("fail to build and expr", K(ret));
  } else {
    range_query->field_ = field_expr;
    range_query->scalar_expr_ = scalar_expr;
    range_query->boost_ = setup_boost(boost_expr);
    query = range_query;
  }
  return ret;
}

int ObDSLResolver::resolve_rank(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  ObString key;
  ObIJsonBase *sub_node = nullptr;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT || req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "rank, should be single-key object");
    LOG_WARN("rank should be single-key object", K(ret));
  } else if (OB_FAIL(req_node.get_object_value(0, key, sub_node))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (key.case_compare("rrf") == 0) {
    if (OB_FAIL(resolve_rrf(*sub_node))) {
      LOG_WARN("fail to resolve rrf", K(ret));
    }
  } else if (key.case_compare("weighted_sum") == 0) {
    if (OB_FAIL(resolve_weighted_sum(*sub_node))) {
      LOG_WARN("fail to resolve weighted_sum", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid rank method");
    LOG_WARN("unsupported rank method", K(ret), K(key));
  }
  if (OB_SUCC(ret)) {
    int64_t window_size_value = 0;
    ObConstRawExpr *window_size_expr = static_cast<ObConstRawExpr *>(dsl_query_info_->rank_info_.window_size_);
    if (OB_FAIL(window_size_expr->get_value().get_int(window_size_value))) {
      LOG_WARN("fail to get int value from window size expr", K(ret));
    } else if (window_size_value < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "window_size, window_size value should be a non-negative integer");
      LOG_WARN("window_size value should be a non-negative integer", K(ret), K(window_size_value));
    } else if (window_size_value > SIZE_VALUE_MAX) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                     "rank.rank_window_size, rank.rank_window_size should be in range [0, 10000]");
      LOG_WARN("window_size value should be in range [0, 10000]", K(ret), K(window_size_value));
    }
  }
  return ret;
}

// Resolve "rerank" JSON: { "model", "field", "query", "rank_window_size", "type"(optional) }
// req_node: the rerank object node from top-level DSL param
int ObDSLResolver::resolve_rerank(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  ObDSLRerankInfo *rerank_info = nullptr;
  uint64_t count = 0;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "rerank, should be object");
    LOG_WARN("rerank must be json object", K(ret), K(req_node.json_type()));
  } else if (OB_ISNULL(rerank_info = OB_NEWx(ObDSLRerankInfo, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate ObDSLRerankInfo", K(ret));
  } else {
    count = req_node.element_count();
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    ObString key;
    ObIJsonBase *sub_node = nullptr;
    if (OB_FAIL(req_node.get_object_value(i, key, sub_node))) {
      LOG_WARN("fail to get value", K(ret), K(i));
    } else if (key.case_compare("model") == 0) {
      if (sub_node->json_type() != ObJsonNodeType::J_STRING) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "rerank.model, should be string");
        LOG_WARN("rerank.model should be string", K(ret), K(sub_node->json_type()));
      } else if (OB_FAIL(construct_string_expr(ObString(sub_node->get_data_length(), sub_node->get_data()), rerank_info->model_))) {
        LOG_WARN("fail to build model expr", K(ret));
      }
    } else if (key.case_compare("query") == 0) {
      if (sub_node->json_type() != ObJsonNodeType::J_STRING) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "rerank.query, should be string");
        LOG_WARN("rerank.query should be string", K(ret), K(sub_node->json_type()));
      } else if (OB_FAIL(construct_string_expr(ObString(sub_node->get_data_length(), sub_node->get_data()), rerank_info->query_))) {
        LOG_WARN("fail to build query expr", K(ret));
      }
    } else if (key.case_compare("field") == 0) {
      ObString col_name;
      ObColumnRefRawExpr *col_expr = nullptr;
      if (sub_node->json_type() != ObJsonNodeType::J_STRING) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "rerank.field, should be string");
        LOG_WARN("rerank.field should be string", K(ret));
      } else if (OB_FALSE_IT(col_name.assign_ptr(sub_node->get_data(), sub_node->get_data_length()))) {
      } else if (OB_FAIL(get_user_column_expr(col_name, col_expr))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_ERR_BAD_FIELD_ERROR;
          LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, col_name.length(), col_name.ptr(),
                         table_item_.get_table_name().length(), table_item_.get_table_name().ptr());
        }
        LOG_WARN("fail to resolve field column", K(ret), K(col_name));
      } else if (!ob_is_string_type(col_expr->get_result_type().get_type())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "rerank.field, should be string or text column");
        LOG_WARN("rerank.field should be string or text column", K(ret), K(col_expr->get_result_type().get_type()));
      } else {
        rerank_info->field_ = col_expr;
      }
    } else if (key.case_compare("rank_window_size") == 0) {
      ObRawExpr *window_size_expr = nullptr;
      ObConstRawExpr *const_expr = nullptr;
      int64_t rws_value = 0;
      if (OB_FAIL(resolve_const(*sub_node, window_size_expr, ObJsonNodeType::J_INT))) {
        LOG_WARN("fail to resolve rank_window_size", K(ret));
      } else if (OB_ISNULL(const_expr = static_cast<ObConstRawExpr *>(window_size_expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null const expr", K(ret));
      } else if (OB_FAIL(const_expr->get_value().get_int(rws_value))) {
        LOG_WARN("fail to get int value from rerank rank_window_size expr", K(ret));
      } else if (rws_value < 0 || rws_value > SIZE_VALUE_MAX) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "rerank.rank_window_size, should be in range [0, 10000]");
        LOG_WARN("rerank.rank_window_size should be in range [0, 10000]", K(ret), K(rws_value));
      } else {
        rerank_info->rank_window_size_ = const_expr;
      }
    } else if (key.case_compare("type") == 0) {
      ObString type_str;
      if (sub_node->json_type() != ObJsonNodeType::J_STRING) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "rerank.type, should be string");
        LOG_WARN("rerank.type should be string", K(ret), K(sub_node->json_type()));
      } else if (OB_FALSE_IT(type_str.assign_ptr(sub_node->get_data(), sub_node->get_data_length()))) {
      } else if (0 != type_str.case_compare("model")) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "rerank.type, should be \"model\"");
        LOG_WARN("rerank.type should be \"model\"", K(ret), K(type_str));
      } else if (OB_FAIL(construct_string_expr(type_str, rerank_info->type_))) {
        LOG_WARN("fail to build rerank type", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "keys other than \"model\", \"query\", \"field\", \"rank_window_size\" and \"type\" in rerank");
      LOG_WARN("unsupported key in rerank", K(ret), K(key));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(rerank_info->model_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "rerank, model is required");
    LOG_WARN("rerank model is required", K(ret));
  } else if (OB_ISNULL(rerank_info->query_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "rerank, query is required");
    LOG_WARN("rerank query is required", K(ret));
  } else if (OB_ISNULL(rerank_info->field_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "rerank, field is required");
    LOG_WARN("rerank field is required", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(rerank_info->rank_window_size_)) {
    ObConstRawExpr *default_window = nullptr;
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, RANK_WINDOW_SIZE_DEFAULT, default_window))) {
      LOG_WARN("fail to build default rank_window_size", K(ret));
    } else {
      rerank_info->rank_window_size_ = default_window;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(rerank_info->type_)) {
    if (OB_FAIL(construct_string_expr(ObString("model"), rerank_info->type_))) {
      LOG_WARN("fail to build default rerank type", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    dsl_query_info_->rank_info_.rerank_info_ = rerank_info;
  }
  return ret;
}

int ObDSLResolver::resolve_rrf(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "rrf, should be object");
    LOG_WARN("rrf should be object", K(ret));
  } else {
    count = req_node.element_count();
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    ObString key;
    ObIJsonBase *sub_node = nullptr;
    if (OB_FAIL(req_node.get_object_value(i, key, sub_node))) {
      LOG_WARN("fail to get value", K(ret), K(i));
    } else if (key.case_compare("rank_constant") == 0) {
      ObRawExpr *rank_const_expr = nullptr;
      ObConstRawExpr *const_expr = nullptr;
      int64_t value = 0;
      if (OB_FAIL(resolve_const(*sub_node, rank_const_expr, ObJsonNodeType::J_INT))) {
        LOG_WARN("fail to parse rank constant value", K(ret), K(i));
      } else if (OB_ISNULL(const_expr = static_cast<ObConstRawExpr*>(rank_const_expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null const expr", K(ret));
      } else if (OB_FAIL(const_expr->get_value().get_int(value))) {
        LOG_WARN("fail to get rank constant value", K(ret), K(i));
      } else if (value < 1) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid rank constant value", K(ret), K(value));
      } else {
        dsl_query_info_->rank_info_.rank_const_ = const_expr;
      }
    } else if (key.case_compare("rank_window_size") == 0) {
      ObRawExpr *window_size_expr = nullptr;
      if (OB_FAIL(resolve_const(*sub_node, window_size_expr, ObJsonNodeType::J_INT))) {
        LOG_WARN("fail to parse rank_window_size value", K(ret), K(i));
      } else {
        dsl_query_info_->rank_info_.window_size_ = window_size_expr;
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid key in rrf");
      LOG_WARN("unsupported key in rrf", K(ret), K(key));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FALSE_IT(dsl_query_info_->rank_info_.method_ = ObFusionMethod::RRF)) {
  } else if (OB_ISNULL(dsl_query_info_->rank_info_.rank_const_)) {
    ObConstRawExpr *tmp_rank_const_expr = nullptr;
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*params_.expr_factory_, ObIntType, RANK_CONST_DEFAULT, tmp_rank_const_expr))) {
      LOG_WARN("fail to create default rank const expr", K(ret));
    } else {
      dsl_query_info_->rank_info_.rank_const_ = tmp_rank_const_expr;
    }
  }
  return ret;
}

int ObDSLResolver::resolve_term(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  ObEsQueryItem query_type = QUERY_ITEM_TERM;
  ObString col_name;
  ObString path_str;
  ObJsonNodeType value_type = ObJsonNodeType::J_MAX_TYPE;
  bool is_array_col = false;
  ObObjType array_base_type = ObMaxType;
  ObIJsonBase *col_para = nullptr;
  ObIJsonBase *value_node = nullptr;
  ObColumnRefRawExpr *col_expr = nullptr;
  ObRawExpr *field_expr = nullptr;
  ObRawExpr *value_expr = nullptr;
  ObRawExpr *expr = nullptr;
  ObConstRawExpr *boost_expr = nullptr;
  ObDSLScalarQuery *term_query = nullptr;
  if (ObDSLQuery::check_need_cal_score_in_bool(outer_query_type, parent_query)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "scalar term query in must/should clause");
    LOG_WARN("scalar term query cannot be scored or exist in must/should clause", K(ret), K(outer_query_type));
  } else if (req_node.json_type() != ObJsonNodeType::J_OBJECT || req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "term query, should be single-key object");
    LOG_WARN("term query should be single-key object", K(ret));
  } else if (OB_FAIL(req_node.get_object_value(0, col_name, col_para))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (OB_FAIL(get_field_expr_and_path(col_name, col_expr, path_str))) {
    LOG_WARN("fail to get field expr and path", K(ret), K(col_name));
  } else if (is_scalar_json_type(col_para->json_type())) {
    value_node = col_para;
  } else if (col_para->json_type() == ObJsonNodeType::J_OBJECT) {
    for (uint64_t i = 0; OB_SUCC(ret) && i < col_para->element_count(); i++) {
      ObString key;
      ObIJsonBase *sub_node = nullptr;
      if (OB_FAIL(col_para->get_object_value(i, key, sub_node))) {
        LOG_WARN("fail to get value", K(ret));
      } else if (key.case_compare("value") == 0) {
        value_node = sub_node;
      } else if (key.case_compare("boost") == 0) {
        if (OB_FAIL(resolve_boost(*sub_node, boost_expr, QUERY_ITEM_TERM, outer_query_type))) {
          LOG_WARN("fail to resolve boost", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "keys other than \"value\" and \"boost\" in term query");
        LOG_WARN("unsupported key in term query", K(ret), K(key));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "term query field value, should be scalar or object");
    LOG_WARN("term query field value should be scalar or object", K(ret), K(col_para->json_type()));
  }
  if (OB_FAIL(ret)) {
  } else if (col_expr->get_result_type().is_json()) {
    query_type = QUERY_ITEM_JSON_CONTAINS;
  } else if (OB_FAIL(is_array_column(col_expr, is_array_col, array_base_type))) {
    LOG_WARN("fail to check if column is array", K(ret), KPC(col_expr));
  } else if (is_array_col) {
    query_type = QUERY_ITEM_ARRAY_CONTAINS;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(value_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "term query, value not exists");
    LOG_WARN("value not exists in term query", K(ret));
  } else if (!is_scalar_json_type(value_node->json_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "value in term query, should be scalar like string, number, boolean, etc.");
    LOG_WARN("invalid value type in term query", K(ret), K(col_expr->get_result_type()), K(value_node->json_type()));
  } else if (OB_FALSE_IT(value_type = resolve_type_mapping(value_node->json_type(), col_expr, query_type, array_base_type))) {
  } else if (query_type != QUERY_ITEM_JSON_CONTAINS &&
             OB_FAIL(resolve_const(*value_node, value_expr, value_type, query_type, array_base_type))) {
    LOG_WARN("fail to resolve const value", K(ret));
  } else if (is_array_col) {
    ObSEArray<ObRawExpr*, 1, ModulePageAllocator, true> value_exprs;
    if (OB_FAIL(value_exprs.push_back(value_expr))) {
      LOG_WARN("fail to push value expr for array_contains", K(ret));
    } else if (OB_FAIL(build_array_intersects_expr(col_expr, value_exprs, expr))) {
      LOG_WARN("fail to build array intersects expr for term", K(ret));
    }
  } else if (OB_FAIL(build_field_expr_with_path(col_expr, path_str, QUERY_ITEM_TERM, field_expr))) {
    LOG_WARN("fail to build field expr with path for term", K(ret));
  } else if (col_expr->get_result_type().is_json()) {
    if (OB_FAIL(build_json_contains_scalar_expr(field_expr, *value_node, expr))) {
      LOG_WARN("fail to build json contains scalar expr for term", K(ret));
    }
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(*expr_factory, T_OP_EQ, field_expr, value_expr, expr))) {
    LOG_WARN("fail to build equal expr", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDSLScalarQuery::create(*allocator_, term_query, QUERY_ITEM_TERM, outer_query_type, parent_query))) {
    LOG_WARN("fail to create term query", K(ret));
  } else {
    term_query->field_ = field_expr;
    term_query->scalar_expr_ = expr;
    term_query->boost_ = setup_boost(boost_expr);
    query = term_query;
  }
  return ret;
}

int ObDSLResolver::get_wildcard_pattern_from_json_node(ObIJsonBase &node, ObString &pattern_str)
{
  int ret = OB_SUCCESS;
  ObJsonBuffer j_buf(allocator_);
  const ObJsonNodeType json_type = node.json_type();
  if (json_type == ObJsonNodeType::J_STRING) {
    pattern_str.assign_ptr(node.get_data(), node.get_data_length());
  } else if (ObIJsonBase::is_json_number_type(json_type) || json_type == ObJsonNodeType::J_BOOLEAN) {
    if (OB_FAIL(print_json_node(node, j_buf))) {
      LOG_WARN("fail to print scalar json node to wildcard pattern string", K(ret), K(json_type));
    } else if (OB_FAIL(j_buf.get_result_string(pattern_str))) {
      LOG_WARN("fail to get wildcard pattern string from buffer", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "wildcard pattern should be string, number or boolean");
    LOG_WARN("wildcard pattern should be string, number or boolean", K(ret), K(json_type));
  }
  return ret;
}

int ObDSLResolver::resolve_wildcard(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  ObString col_name;
  ObString path_str;
  ObIJsonBase *col_para = nullptr;
  ObIJsonBase *pattern_node = nullptr;
  ObColumnRefRawExpr *col_expr = nullptr;
  ObRawExpr *path_expr = nullptr;
  ObSysFunRawExpr *json_extract_expr = nullptr;
  ObSysFunRawExpr *json_unquote_expr = nullptr;
  ObRawExpr *field_expr = nullptr;
  ObRawExpr *pattern_expr = nullptr;
  ObRawExpr *escape_raw_expr = nullptr;
  ObConstRawExpr *escape_expr = nullptr;
  ObOpRawExpr *like_expr = nullptr;
  ObConstRawExpr *boost_expr = nullptr;
  ObDSLScalarQuery *wildcard_query = nullptr;
  ObCollationType target_coll = CS_TYPE_INVALID;
  ObString wildcard_pattern;
  ObString like_pattern;
  if (ObDSLQuery::check_need_cal_score_in_bool(outer_query_type, parent_query)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "scalar wildcard query in must/should clause");
    LOG_WARN("scalar wildcard query cannot be scored or exist in must/should clause", K(ret), K(outer_query_type));
  } else if (req_node.json_type() != ObJsonNodeType::J_OBJECT || req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "wildcard query, should be single-key object");
    LOG_WARN("wildcard query should be single-key object", K(ret));
  } else if (OB_FAIL(req_node.get_object_value(0, col_name, col_para))) {
    LOG_WARN("failed to get wildcard field", K(ret));
  } else if (OB_FAIL(get_field_expr_and_path(col_name, col_expr, path_str))) {
    LOG_WARN("failed to get field expr and path", K(ret), K(col_name));
  } else if (path_str.empty() && !ob_is_string_or_lob_type(col_expr->get_result_type().get_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-json column not string or lob type using wildcard query");
    LOG_WARN("non-json column not string or lob type using wildcard query is not supported", K(ret), K(col_name), K(col_expr->get_result_type()));
  } else if (col_para->json_type() == ObJsonNodeType::J_STRING
             || ObIJsonBase::is_json_number_type(col_para->json_type())
             || col_para->json_type() == ObJsonNodeType::J_BOOLEAN) {
    pattern_node = col_para;
  } else if (col_para->json_type() == ObJsonNodeType::J_OBJECT) {
    if (col_para->element_count() == 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "param of wildcard, should not be empty");
      LOG_WARN("param of wildcard should not be empty", K(ret));
    }
    for (uint64_t i = 0; OB_SUCC(ret) && i < col_para->element_count(); ++i) {
      ObString key;
      ObIJsonBase *sub_node = nullptr;
      if (OB_FAIL(col_para->get_object_value(i, key, sub_node))) {
        LOG_WARN("failed to get wildcard param", K(ret), K(i));
      } else if (key.case_compare("value") == 0 || key.case_compare("wildcard") == 0) {
        if (OB_NOT_NULL(pattern_node)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "wildcard query, only one of 'value' or 'wildcard' can be specified");
          LOG_WARN("wildcard query, only one of 'value' or 'wildcard' can be specified", K(ret));
        } else if (sub_node->json_type() != ObJsonNodeType::J_STRING
            && !ObIJsonBase::is_json_number_type(sub_node->json_type())
            && sub_node->json_type() != ObJsonNodeType::J_BOOLEAN) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "value type in json column wildcard query, should be string, number or boolean");
          LOG_WARN("value type in json column wildcard query should be string, number or boolean",
                   K(ret), K(key), K(sub_node->json_type()));
        } else {
          pattern_node = sub_node;
        }
      } else if (key.case_compare("boost") == 0) {
        if (OB_FAIL(resolve_boost(*sub_node, boost_expr, QUERY_ITEM_WILDCARD, outer_query_type))) {
          LOG_WARN("fail to resolve boost", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "keys other than \"value\", \"wildcard\" and \"boost\" in wildcard expr");
        LOG_WARN("unsupported key in wildcard query", K(ret), K(key));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "wildcard field value should be string, number, boolean or object");
    LOG_WARN("wildcard field value should be string, number, boolean or object", K(ret), K(col_para->json_type()));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(pattern_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "wildcard query, required param 'value' or 'wildcard' is missing");
    LOG_WARN("wildcard pattern is missing", K(ret));
  } else if (OB_FAIL(get_wildcard_pattern_from_json_node(*pattern_node, wildcard_pattern))) {
    LOG_WARN("failed to get wildcard pattern from json node", K(ret));
  } else if (OB_FAIL(convert_wildcard_pattern_to_like(wildcard_pattern, like_pattern))) {
    LOG_WARN("failed to convert wildcard pattern", K(ret));
  } else if (OB_FAIL(build_field_expr_with_path(col_expr, path_str, QUERY_ITEM_WILDCARD, field_expr))) {
    LOG_WARN("failed to build field expr with path for wildcard", K(ret), K(path_str));
  } else if (path_str.empty()) {
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SYS_JSON_UNQUOTE, json_unquote_expr))) {
    LOG_WARN("failed to create json_unquote expr", K(ret));
  } else if (OB_FALSE_IT(json_unquote_expr->set_func_name(N_JSON_UNQUOTE))) {
  } else if (OB_FAIL(json_unquote_expr->set_param_expr(field_expr))) {
    LOG_WARN("failed to set param exprs for json_unquote", K(ret));
  } else {
    field_expr = json_unquote_expr;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FALSE_IT(target_coll = field_expr->get_collation_type())) {
  } else if (OB_FAIL(construct_string_expr(like_pattern, pattern_expr, target_coll))) {
    LOG_WARN("failed to create wildcard pattern expr", K(ret), K(like_pattern), K(target_coll));
  } else if (OB_FAIL(construct_string_expr(ObString::make_string("\\"), escape_raw_expr, target_coll))) {
    LOG_WARN("failed to create escape expr", K(ret));
  } else if (OB_ISNULL(escape_expr = static_cast<ObConstRawExpr *>(escape_raw_expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("escape expr must be const expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_like_expr(*expr_factory, session_info_, field_expr, pattern_expr, escape_expr, like_expr))) {
    LOG_WARN("failed to build like expr", K(ret));
  } else if (OB_FAIL(ObDSLScalarQuery::create(*allocator_, wildcard_query, QUERY_ITEM_WILDCARD, outer_query_type, parent_query))) {
    LOG_WARN("failed to create wildcard query", K(ret));
  } else {
    wildcard_query->field_ = field_expr;
    wildcard_query->scalar_expr_ = like_expr;
    wildcard_query->boost_ = setup_boost(boost_expr);
    query = wildcard_query;
  }
  return ret;
}

int ObDSLResolver::resolve_terms(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  ObEsQueryItem query_type = QUERY_ITEM_TERMS;
  uint64_t count = 0;
  ObString path_str;
  bool is_array_col = false;
  ObObjType array_base_type = ObMaxType;
  ObColumnRefRawExpr *col_expr = nullptr;
  ObRawExpr *field_expr = nullptr;
  ObIJsonBase *array_elem_node = nullptr; // array element in terms param array
  ObSEArray<ObRawExpr*, 4, ModulePageAllocator, true> value_exprs;
  ObRawExpr *expr = nullptr;
  ObConstRawExpr *boost_expr = nullptr;
  ObDSLScalarQuery *terms_query = nullptr;
  if (ObDSLQuery::check_need_cal_score_in_bool(outer_query_type, parent_query)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "scalar terms query in must/should clause");
    LOG_WARN("scalar terms query cannot be scored or exist in must/should clause", K(ret), K(outer_query_type));
  } else if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "terms query, should be object");
    LOG_WARN("terms query should be object", K(ret));
  } else {
    count = req_node.element_count();
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    ObString key;
    ObIJsonBase *sub_node = nullptr;
    if (OB_FAIL(req_node.get_object_value(i, key, sub_node))) {
      LOG_WARN("fail to get value", K(ret));
    } else if (key.case_compare("boost") == 0) {
      if (OB_FAIL(resolve_boost(*sub_node, boost_expr, QUERY_ITEM_TERMS, outer_query_type))) {
        LOG_WARN("fail to resolve boost", K(ret));
      }
    } else if (OB_NOT_NULL(col_expr)) {
      if (count == 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col_expr not null but count is 1", K(ret));
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "keys other than field name and \"boost\" in terms query");
        LOG_WARN("unsupported key in terms query", K(ret), K(key));
      }
    } else if (sub_node->json_type() != ObJsonNodeType::J_ARRAY) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "terms query field value, should be array");
      LOG_WARN("terms query field value should be array", K(ret), K(sub_node->json_type()));
    } else if (OB_FAIL(get_field_expr_and_path(key, col_expr, path_str))) {
      LOG_WARN("fail to get field expr and path", K(ret), K(key));
    } else {
      uint64_t array_count = sub_node->element_count();
      if (array_count == 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "terms param array, should have at least one element");
        LOG_WARN("keyword array should have at least one element", K(ret));
      } else if (col_expr->get_result_type().is_json()) {
        if (array_count == 1) {
          query_type = QUERY_ITEM_JSON_CONTAINS;
        } else {
          query_type = QUERY_ITEM_JSON_OVERLAPS;
        }
      } else if (OB_FAIL(is_array_column(col_expr, is_array_col, array_base_type))) {
        LOG_WARN("fail to check if column is array", K(ret), KPC(col_expr));
      } else if (is_array_col) {
        if (array_count == 1) {
          query_type = QUERY_ITEM_ARRAY_CONTAINS;
        } else {
          query_type = QUERY_ITEM_ARRAY_OVERLAPS;
        }
      } else if (array_count == 1) {
        query_type = QUERY_ITEM_TERM;
      }
      for (uint64_t j = 0; OB_SUCC(ret) && j < array_count; j++) {
        ObJsonNodeType element_type = ObJsonNodeType::J_MAX_TYPE;
        ObIJsonBase *element = nullptr;
        ObRawExpr *value_expr = nullptr;
        if (OB_FAIL(sub_node->get_array_element(j, element))) {
          LOG_WARN("fail to get array element", K(ret), K(j));
        } else if (!is_scalar_json_type(element->json_type()) &&
                   (element->json_type() != ObJsonNodeType::J_OBJECT || !col_expr->get_result_type().is_json())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "value in terms query, should be scalar like string, number, boolean, etc.");
          LOG_WARN("invalid value type in terms query", K(ret), K(col_expr->get_result_type()), K(element->json_type()));
        } else if (query_type == QUERY_ITEM_JSON_CONTAINS) {
          if (array_count != 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("json contains terms should have only one element", K(ret));
          } else {
            // for json_contains, record the only element node for special handling later and break
            array_elem_node = element;
            break;
          }
        } else if (OB_FALSE_IT(element_type = resolve_type_mapping(element->json_type(), col_expr, query_type, array_base_type))) {
        } else if (OB_FAIL(resolve_const(*element, value_expr, element_type, query_type, array_base_type))) {
          LOG_WARN("fail to resolve const value", K(ret), K(j));
        } else if (OB_FAIL(value_exprs.push_back(value_expr))) {
          LOG_WARN("fail to add value to value_exprs", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(col_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "terms query, it should have field at least");
    LOG_WARN("terms expr should have field", K(ret));
  } else if (is_array_col) {
    if (OB_FAIL(build_array_intersects_expr(col_expr, value_exprs, expr))) {
      LOG_WARN("fail to build array intersects expr for terms", K(ret));
    }
  } else if (OB_FAIL(build_field_expr_with_path(col_expr, path_str, query_type, field_expr))) {
    LOG_WARN("fail to build field expr with path for terms", K(ret));
  } else if (col_expr->get_result_type().is_json()) {
    // for single-value, JSON_CONTAINS is closer to ES with a narrow performance gap compared to JSON_OVERLAPS
    // for multiple values, JSON_OVERLAPS is approximately ES-compatible, but has better performance
    if (query_type == QUERY_ITEM_JSON_CONTAINS) {
      if (OB_FAIL(build_json_contains_scalar_expr(field_expr, *array_elem_node, expr))) {
        LOG_WARN("fail to build json contains scalar expr for terms on json column", K(ret));
      }
    } else if (OB_FAIL(build_json_overlaps_array_expr(field_expr, value_exprs, expr))) {
      LOG_WARN("fail to build json intersects array expr for terms on json column", K(ret));
    }
  } else if (query_type == QUERY_ITEM_TERM) {
    if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(*expr_factory, T_OP_EQ, field_expr, value_exprs.at(0), expr))) {
      LOG_WARN("fail to build equal expr", K(ret));
    }
  } else { // query_type == QUERY_ITEM_TERMS
    ObOpRawExpr *row_expr = nullptr;
    if (OB_FAIL(expr_factory->create_raw_expr(T_OP_ROW, row_expr))) {
      LOG_WARN("fail to create row expr", K(ret));
    } else if (OB_FAIL(row_expr->set_param_exprs(value_exprs))) {
      LOG_WARN("fail to set param exprs for row expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(*expr_factory, T_OP_IN, field_expr, row_expr, expr))) {
      LOG_WARN("fail to build in expr", K(ret));
    } else {
      static_cast<ObOpRawExpr*>(expr)->set_add_implicit_cast_for_in_param(true);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDSLScalarQuery::create(*allocator_, terms_query, query_type, outer_query_type, parent_query))) {
    LOG_WARN("fail to create terms query", K(ret));
  } else {
    terms_query->field_ = field_expr;
    terms_query->scalar_expr_ = expr;
    terms_query->boost_ = setup_boost(boost_expr);
    query = terms_query;
  }
  return ret;
}

int ObDSLResolver::resolve_weighted_sum(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  ObString normalizer_str;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "weighted_sum, should be object");
    LOG_WARN("weighted_sum should be object", K(ret));
  } else {
    count = req_node.element_count();
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    ObString key;
    ObIJsonBase *sub_node = nullptr;
    if (OB_FAIL(req_node.get_object_value(i, key, sub_node))) {
      LOG_WARN("fail to get value", K(ret), K(i));
    } else if (key.case_compare("normalizer") == 0) {
      if (sub_node->json_type() != ObJsonNodeType::J_STRING) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "weighted_sum.normalizer, should be string");
        LOG_WARN("weighted_sum.normalizer should be string", K(ret), K(sub_node->json_type()));
      } else if (OB_FALSE_IT(normalizer_str = ObString(sub_node->get_data_length(), sub_node->get_data()))) {
      } else if (normalizer_str.case_compare("minmax") == 0) {
        dsl_query_info_->rank_info_.method_ = ObFusionMethod::MINMAX_NORMALIZER;
      } else if (normalizer_str.case_compare("none") == 0) {
        dsl_query_info_->rank_info_.method_ = ObFusionMethod::WEIGHT_SUM;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid normalizer value");
        LOG_WARN("invalid normalizer value", K(ret), K(normalizer_str));
      }
    } else if (key.case_compare("rank_window_size") == 0) {
      ObRawExpr *window_size_expr = nullptr;
      if (OB_FAIL(resolve_const(*sub_node, window_size_expr, ObJsonNodeType::J_INT))) {
        LOG_WARN("fail to parse rank_window_size value", K(ret), K(i));
      } else {
        dsl_query_info_->rank_info_.window_size_ = window_size_expr;
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "unsupported key in weighted_sum");
      LOG_WARN("unsupported key in weighted_sum", K(ret), K(key));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (normalizer_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("normalizer is required in weighted_sum", K(ret));
  }
  return ret;
}

int ObDSLResolver::setup_top_level_score(ObDSLQuery *query)
{
  int ret = OB_SUCCESS;
  if (!query->need_cal_score_) {
  } else if (IS_QUERY_ITEM_FULLTEXT(query->query_type_) ||
             IS_QUERY_ITEM_SCALAR(query->query_type_) ||
             IS_QUERY_ITEM_JSON(query->query_type_) ||
             IS_QUERY_ITEM_ARRAY(query->query_type_)) {
    query->is_top_level_score_ = true;
  } else if (IS_QUERY_ITEM_BOOL(query->query_type_)) {
    ObDSLBoolQuery *bool_query = static_cast<ObDSLBoolQuery*>(query);
    ObDSLQuery *only_score_query = nullptr;
    for (int64_t i = 0; i < bool_query->must_.count(); i++) {
      if (bool_query->must_.at(i)->need_cal_score_) {
        if (OB_ISNULL(only_score_query)) {
          only_score_query = bool_query->must_.at(i);
        } else {
          query->is_top_level_score_ = true;
          break;
        }
      }
    }
    for (int64_t i = 0; !query->is_top_level_score_ && i < bool_query->should_.count(); i++) {
      if (bool_query->should_.at(i)->need_cal_score_) {
        if (OB_ISNULL(only_score_query)) {
          only_score_query = bool_query->should_.at(i);
        } else {
          query->is_top_level_score_ = true;
          break;
        }
      }
    }
    if (OB_ISNULL(only_score_query) || query->is_top_level_score_) {
    } else if (OB_FAIL(setup_top_level_score(only_score_query))) {
      LOG_WARN("fail to setup top level score for score query", K(ret));
    }
  }
  return ret;
}

int ObDSLResolver::set_default_rank_window_size()
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  int64_t size_val = 0;
  int64_t from_val = 0;
  ObConstRawExpr *sum_expr = nullptr;
  if (OB_ISNULL(dsl_query_info_->size_) || OB_ISNULL(dsl_query_info_->from_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr factory or size/from expr is null", K(ret));
  } else if (OB_FAIL(static_cast<ObConstRawExpr *>(dsl_query_info_->size_)->get_value().get_int(size_val))) {
    LOG_WARN("failed to get size const value", K(ret));
  } else if (OB_FAIL(static_cast<ObConstRawExpr *>(dsl_query_info_->from_)->get_value().get_int(from_val))) {
    LOG_WARN("failed to get from const value", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, size_val + from_val, sum_expr))) {
    LOG_WARN("failed to build size+from const expr", K(ret));
  } else {
    dsl_query_info_->rank_info_.window_size_ = sum_expr;
  }
  return ret;
}

// Boosts need to be calculated before topk collection.
// Here we push cumulative nested boosts to leaf queries to avoid redundant relevance boost calculation.
int ObDSLResolver::try_push_nested_boost_to_leaf_query(ObDSLQuery *query, const double cumulative_boost)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query is null", K(ret));
  } else if (IS_QUERY_ITEM_FULLTEXT(query->query_type_) ||
             IS_QUERY_ITEM_ARRAY(query->query_type_) ||
             IS_QUERY_ITEM_JSON(query->query_type_) ||
             IS_QUERY_ITEM_SCALAR(query->query_type_)) {
    if (OB_ISNULL(query->boost_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null boost expr", K(ret));
    } else {
      const double new_boost = cumulative_boost * query->boost_->get_value().get_double();
      // if boost_ is the shared one_const_expr_, we need to create a new expression
      // instead of modifying the shared one, to avoid affecting other queries that use it
      if (query->boost_ == dsl_query_info_->one_const_expr_) {
        ObConstRawExpr *new_boost_expr = nullptr;
        ObRawExprFactory *expr_factory = params_.expr_factory_;
        if (OB_FAIL(ObRawExprUtils::build_const_double_expr(*expr_factory, ObDoubleType, new_boost, new_boost_expr))) {
          LOG_WARN("fail to create new boost expr", K(ret));
        } else {
          query->boost_ = new_boost_expr;
        }
      } else {
        ObObj new_boost_obj;
        new_boost_obj.set_double(new_boost);
        query->boost_->set_value(new_boost_obj);
      }
    }
  } else if (IS_QUERY_ITEM_BOOL(query->query_type_)) {
    // accumulate & propagate boost
    ObDSLBoolQuery *bool_query = static_cast<ObDSLBoolQuery*>(query);
    double new_boost = cumulative_boost * bool_query->boost_->get_value().get_double();
    for (int64_t i = 0; OB_SUCC(ret) && i < bool_query->must_.count(); i++) {
      if (OB_FAIL(try_push_nested_boost_to_leaf_query(bool_query->must_.at(i), new_boost))) {
        LOG_WARN("fail to push nested boost to leaf query", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < bool_query->should_.count(); i++) {
      if (OB_FAIL(try_push_nested_boost_to_leaf_query(bool_query->should_.at(i), new_boost))) {
        LOG_WARN("fail to push nested boost to leaf query", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      bool_query->boost_ = static_cast<ObConstRawExpr*>(dsl_query_info_->one_const_expr_);
    }
  } else {
    // query types irrelevant to nested boost
  }
  return ret;
}

int ObDSLResolver::resolve_collapse(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  ObString field_name;
  ObIJsonBase *field_node = nullptr;
  ObColumnRefRawExpr *col_expr = nullptr;
  if (dsl_query_info_->has_dsl_aggs()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "aggs with collapse");
    LOG_WARN("collapse and aggs cannot be used together", K(ret));
  } else if (req_node.json_type() != ObJsonNodeType::J_OBJECT || req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "collapse, should be single-key object");
    LOG_WARN("collapse should be single-key object", K(ret));
  } else if (OB_FAIL(req_node.get_object_value("field", field_node))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "collapse, \"field\" is required");
    LOG_WARN("collapse, \"field\" is required", K(ret));
  } else if (field_node->json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "collapse.field, should be string");
    LOG_WARN("collapse.field should be string", K(ret));
  } else if (FALSE_IT(field_name.assign_ptr(field_node->get_data(), field_node->get_data_length()))) {
  } else if (field_name.case_compare("__score") == 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "collapse by __score");
    LOG_WARN("collapse by __score is not supported", K(ret));
  } else if (OB_FAIL(get_user_column_expr(field_name, col_expr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, field_name.length(), field_name.ptr(),
                     table_item_.get_table_name().length(), table_item_.get_table_name().ptr());
    }
    LOG_WARN("fail to resolve collapse field", K(ret), K(field_name));
  } else {
    ObColumnIndexInfo *idx_info = nullptr;
    if (OB_FAIL(get_col_idx_info(col_expr->get_column_name(), idx_info))) {
      LOG_WARN("fail to get col idx info for collapse field", K(ret), K(field_name));
    } else if (OB_NOT_NULL(idx_info)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "collapse by fulltext/vector index column");
      LOG_WARN("collapse on domain index column not supported", K(ret), K(field_name));
    } else if (!is_groupable_type(col_expr->get_result_type().get_type())
               || col_expr->get_result_type().is_lob()
               || col_expr->get_result_type().is_json()
               || col_expr->get_result_type().is_geometry()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "collapse by text/json/geometry column type");
      LOG_WARN("unsupported collapse column type", K(ret), K(field_name), K(col_expr->get_result_type()));
    } else {
      dsl_query_info_->collapse_info_.field_ = col_expr;
    }
  }
  return ret;
}

int ObDSLResolver::inject_collapse_stmt_rewrites()
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  ObSelectStmt *select_stmt = nullptr;
  ObWinFunRawExpr *row_num_expr = nullptr;
  ObConstRawExpr *one_expr = nullptr;
  ObRawExpr *qualify_expr = nullptr;
  ObSEArray<ObRawExpr *, 1> partition_exprs;
  if (!dsl_query_info_->has_dsl_collapse()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("collapse field is null", K(ret));
  } else if (dsl_query_info_->has_dsl_aggs() ||
             dsl_query_info_->result_mode_ != ObDSLResultMode::SEARCH_HITS) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "use collapse with aggs");
    LOG_WARN("collapse and aggs cannot be used together", K(ret));
  } else if (FALSE_IT(select_stmt = static_cast<ObSelectStmt *>(stmt_))) {
  } else if (OB_FAIL(partition_exprs.push_back(dsl_query_info_->collapse_info_.field_))) {
    LOG_WARN("fail to push back collapse partition expr", K(ret));
  } else if (select_stmt->get_order_item_size() == 0) {
    ObRawExpr *fusion_score_expr = nullptr;
    if (dsl_query_info_->score_cols_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("score columns not available for collapse", K(ret));
    } else if (OB_ISNULL(fusion_score_expr = dsl_query_info_->score_cols_.at(dsl_query_info_->score_cols_.count() - 1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fusion score expr is null", K(ret));
    } else {
      OrderItem default_order_item(fusion_score_expr, NULLS_LAST_DESC);
      if (OB_FAIL(select_stmt->add_order_item(default_order_item))) {
        LOG_WARN("fail to add default order item for collapse", K(ret));
      } else if (OB_FAIL(set_stmt_limit_offset(dsl_query_info_, *select_stmt))) {
        LOG_WARN("failed to set stmt limit for collapse", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (select_stmt->get_order_item_size() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("collapse order items should not be empty", K(ret));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_WINDOW_FUNCTION, row_num_expr))) {
    LOG_WARN("fail to create row_number window expr", K(ret));
  } else if (OB_FAIL(row_num_expr->set_partition_exprs(partition_exprs))) {
    LOG_WARN("fail to set collapse partition exprs", K(ret));
  } else if (OB_FALSE_IT(row_num_expr->set_func_type(T_WIN_FUN_ROW_NUMBER))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_order_item_size(); ++i) {
      if (OB_FAIL(row_num_expr->get_order_items().push_back(select_stmt->get_order_item(i)))) {
        LOG_WARN("fail to push back collapse order item", K(ret), K(i));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, 1, one_expr))) {
    LOG_WARN("fail to build const expr for collapse filter", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(*expr_factory, T_OP_EQ,
                                                                 row_num_expr, one_expr,
                                                                 qualify_expr))) {
    LOG_WARN("fail to build qualify expr for collapse", K(ret));
  } else {
    dsl_query_info_->collapse_info_.win_func_expr_ = row_num_expr;
    dsl_query_info_->collapse_info_.qualify_expr_ = qualify_expr;
  }
  return ret;
}

int ObDSLResolver::has_user_column_name(const ObString &col_name, bool &exists, ObColumnRefRawExpr **col_expr/**=nullptr*/)
{
  int ret = OB_SUCCESS;
  ObString lower_name;
  ColumnItem *existing_col_item = nullptr;
  const ObColumnSchemaV2 *col_schema = nullptr;
  exists = false;
  if (OB_FAIL(ObCharset::tolower(CS_TYPE_UTF8MB4_GENERAL_CI, col_name, lower_name, *allocator_))) {
    LOG_WARN("fail to lower column name", K(ret), K(col_name));
  } else if (OB_NOT_NULL(existing_col_item = stmt_->get_column_item(table_item_.table_id_, lower_name))) {
    exists = true;
    if (OB_NOT_NULL(col_expr)) {
      *col_expr = existing_col_item->expr_;
    }
  } else if (OB_FAIL(col_schema_map_.get_refactored(lower_name, col_schema))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to lookup column name in schema map", K(ret), K(col_name));
    }
  } else if (OB_FALSE_IT(exists = true)) {
  } else if (OB_NOT_NULL(col_expr)) {
    ObRawExprFactory *expr_factory = params_.expr_factory_;
    if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *col_schema, session_info_, *col_expr))) {
      LOG_WARN("failed to build column expr", K(ret), K(col_schema->get_column_id()), K(col_name));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(col_expr) || !exists) {
    // do nothing
  } else if (OB_ISNULL(*col_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("col_expr is null despite column being found", K(ret));
  } else if ((*col_expr)->is_virtual_generated_column()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "virtual generated column in dsl");
    LOG_WARN("virtual generated column is not supported", K(ret), K(lower_name));
  } else {
    setup_column_expr_attr(*col_expr);
    if (OB_ISNULL(existing_col_item)) {
      ColumnItem column_item;
      column_item.expr_ = *col_expr;
      column_item.table_id_ = table_item_.table_id_;
      column_item.column_id_ = (*col_expr)->get_column_id();
      column_item.column_name_ = (*col_expr)->get_column_name();
      column_item.base_tid_ = table_item_.ref_id_;
      column_item.base_cid_ = column_item.column_id_;
      column_item.is_geo_ = col_schema->is_geometry();
      column_item.set_default_value(col_schema->get_cur_default_value());
      if (OB_FAIL(stmt_->add_column_item(column_item))) {
        LOG_WARN("fail to add column item to stmt", K(ret), K(lower_name));
      }
    }
  }
  return ret;
}

// JSON DSL text parsing produces J_DOUBLE for floating-point numbers.
// Truncate JSON double to int64/uint64 using trunc() (toward zero), aligned with ES cast-to-long.
// Upper bound uses >= because LLONG_MAX/ULLONG_MAX round up in double precision.
int ObDSLResolver::trunc_json_float_to_int(ObIJsonBase &json_node, bool is_unsigned,
                                           int64_t &int_val, uint64_t &uint_val)
{
  int ret = OB_SUCCESS;
  double dv = json_node.get_double();
  dv = trunc(dv);
  if (isnan(dv) || isinf(dv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("float value is nan or inf", K(ret), K(dv));
  } else if (is_unsigned) {
    if (dv < 0 || dv >= static_cast<double>(ULLONG_MAX)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("float value out of uint64 range", K(ret), K(dv));
    } else {
      uint_val = static_cast<uint64_t>(dv);
    }
  } else {
    if (dv < static_cast<double>(LLONG_MIN) || dv >= static_cast<double>(LLONG_MAX)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("float value out of int64 range", K(ret), K(dv));
    } else {
      int_val = static_cast<int64_t>(dv);
    }
  }
  return ret;
}

int ObDSLResolver::check_aggs_bucket_name(const ObString &agg_name)
{
  int ret = OB_SUCCESS;
  bool name_conflicts = false;
  if (agg_name.length() >= 2 && agg_name.ptr()[0] == '_' && agg_name.ptr()[1] == '_') {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "aggs bucket name, names starting with \"__\" are reserved");
    LOG_WARN("aggs bucket name with __ prefix is reserved", K(ret), K(agg_name));
  } else if (OB_FAIL(has_user_column_name(agg_name, name_conflicts))) {
    LOG_WARN("failed to check agg bucket name conflict", K(ret), K(agg_name));
  } else if (name_conflicts) {
    ret = OB_ERR_COLUMN_DUPLICATE;
    LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, agg_name.length(), agg_name.ptr());
    LOG_WARN("aggs bucket name conflicts with user column", K(ret), K(agg_name));
  }
  return ret;
}

int ObDSLResolver::resolve_aggs(ObIJsonBase &aggs_node)
{
  int ret = OB_SUCCESS;
  ObString agg_name;
  ObString agg_type;
  ObIJsonBase *agg_body = nullptr;
  ObIJsonBase *type_body = nullptr;
  if (dsl_query_info_->has_dsl_collapse()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "aggs with collapse");
    LOG_WARN("aggs and collapse cannot be used together", K(ret));
  } else if (aggs_node.json_type() != ObJsonNodeType::J_OBJECT || aggs_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "aggs, should be single-key object");
    LOG_WARN("aggs should be single-key object", K(ret));
  } else if (OB_FAIL(aggs_node.get_object_value(0, agg_name, agg_body))) {
    LOG_WARN("fail to get agg body", K(ret));
  } else if (agg_body->json_type() != ObJsonNodeType::J_OBJECT || agg_body->element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "aggs body, should be single-key object");
    LOG_WARN("aggs body should be single-key object", K(ret), K(agg_name));
  } else if (OB_FAIL(agg_body->get_object_value(0, agg_type, type_body))) {
    LOG_WARN("fail to get aggregation type", K(ret), K(agg_name));
  } else if (agg_type.case_compare("terms") == 0) {
    if (OB_FAIL(resolve_aggs_terms_or_cardinality(agg_name, *type_body, ObDSLAggTermsItem::TERMS))) {
      LOG_WARN("fail to resolve agg terms", K(ret), K(agg_name));
    }
  } else if (agg_type.case_compare("cardinality") == 0) {
    if (OB_FAIL(resolve_aggs_terms_or_cardinality(agg_name, *type_body, ObDSLAggTermsItem::CARDINALITY))) {
      LOG_WARN("fail to resolve agg cardinality", K(ret), K(agg_name));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "keys other than \"terms\" and \"cardinality\" in aggs");
    LOG_WARN("unsupported aggregation type", K(ret), K(agg_type));
  }
  return ret;
}

int ObDSLResolver::resolve_aggs_field(ObIJsonBase &field_val, ObColumnRefRawExpr *&col_expr)
{
  int ret = OB_SUCCESS;
  ObString col_name;
  ObColumnIndexInfo *idx_info = nullptr;
  if (field_val.json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "aggs.field, should be string");
    LOG_WARN("aggs.field should be string", K(ret));
  } else if (OB_FALSE_IT(col_name.assign_ptr(field_val.get_data(), field_val.get_data_length()))) {
  } else if (OB_FAIL(get_user_column_expr(col_name, col_expr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, col_name.length(), col_name.ptr(),
                     (int)strlen("aggregation"), "aggregation");
    }
    LOG_WARN("fail to get column expr for agg field", K(ret), K(col_name));
  } else if (OB_FAIL(get_col_idx_info(col_expr->get_column_name(), idx_info))) {
    LOG_WARN("fail to get col idx info", K(ret), K(col_name));
  } else if (OB_NOT_NULL(idx_info)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "aggregation on fulltext/vector index column");
    LOG_WARN("agg on domain index column not supported", K(ret), K(col_name));
  } else if (!is_groupable_type(col_expr->get_data_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "aggregation on this column type");
    LOG_WARN("column type not groupable", K(ret), K(col_name), K(col_expr->get_data_type()));
  }
  return ret;
}

int ObDSLResolver::resolve_aggs_terms_or_cardinality(const ObString &agg_name,
                                                     ObIJsonBase &agg_node,
                                                     ObDSLAggTermsItem::AggType agg_type)
{
  int ret = OB_SUCCESS;
  if (agg_type != ObDSLAggTermsItem::TERMS && agg_type != ObDSLAggTermsItem::CARDINALITY) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected agg type", K(ret), K(agg_type));
  } else if (agg_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "agg body, should be object");
    LOG_WARN("agg body should be object", K(ret));
  } else {
    ObDSLAggTermsItem item;
    if (OB_FAIL(check_aggs_bucket_name(agg_name))) {
      LOG_WARN("aggs bucket name check failed", K(ret), K(agg_name));
    } else {
      item.agg_type_ = agg_type;
      // agg_name points into the JSON buffer allocated from allocator_ (statement-level arena),
      // which has the same lifetime as dsl_query_info_. Shallow copy is safe.
      item.agg_name_ = agg_name;
    }
    for (uint64_t i = 0; OB_SUCC(ret) && i < agg_node.element_count(); i++) {
      ObString key;
      ObIJsonBase *val = nullptr;
      if (OB_FAIL(agg_node.get_object_value(i, key, val))) {
        LOG_WARN("fail to get agg param", K(ret), K(i));
      } else if (key.case_compare("field") == 0) {
        ObColumnRefRawExpr *col_expr = nullptr;
        if (OB_FAIL(resolve_aggs_field(*val, col_expr))) {
          LOG_WARN("fail to resolve agg field", K(ret));
        } else {
          item.field_ = col_expr;
        }
      } else if (agg_type == ObDSLAggTermsItem::TERMS) {
        if (key.case_compare("size") == 0) {
          if (val->json_type() != ObJsonNodeType::J_INT) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "agg.terms.size, should be integer");
            LOG_WARN("terms.size should be integer", K(ret));
          } else if (val->get_int() <= 0) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "agg.terms.size, should be > 0");
            LOG_WARN("terms.size should be > 0", K(ret), K(val->get_int()));
          } else {
            item.size_ = val->get_int();
          }
        } else if (key.case_compare("min_doc_count") == 0) {
          if (val->json_type() != ObJsonNodeType::J_INT) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "aggs.terms.min_doc_count, should be integer");
            LOG_WARN("aggs.terms.min_doc_count should be integer", K(ret));
          } else if (val->get_int() <= 0) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "aggs.terms.min_doc_count, should be > 0");
            LOG_WARN("aggs.terms.min_doc_count should be > 0", K(ret), K(item.min_doc_count_));
          } else {
            item.min_doc_count_ = val->get_int();
          }
        } else if (key.case_compare("order") == 0) {
          ObString order_key;
          ObString order_val_str;
          ObIJsonBase *order_val = nullptr;
          if (val->json_type() != ObJsonNodeType::J_OBJECT || val->element_count() != 1) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "aggs.terms.order, should be single-key object");
            LOG_WARN("aggs.terms.order should be single-key object", K(ret));
          } else if (OB_FAIL(val->get_object_value(0, order_key, order_val))) {
            LOG_WARN("fail to get order key", K(ret));
          } else if (order_key.case_compare("_count") == 0) {
            item.order_by_ = ObDSLAggTermsItem::BY_COUNT;
          } else if (order_key.case_compare("_key") == 0) {
            item.order_by_ = ObDSLAggTermsItem::BY_KEY;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "keys other than \"_count\" and \"_key\" in aggs.terms.order");
            LOG_WARN("unsupported key in aggs.terms.order", K(ret), K(order_key));
          }
          if (OB_FAIL(ret)) {
          } else if (order_val->json_type() != ObJsonNodeType::J_STRING) {
            ret = OB_INVALID_ARGUMENT;
            if (item.order_by_ == ObDSLAggTermsItem::BY_COUNT) {
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "agg.terms.order._count, should be string");
              LOG_WARN("agg.terms.order._count should be string", K(ret));
            } else { // BY_KEY
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "agg.terms.order._key, should be string");
              LOG_WARN("agg.terms.order._key should be string", K(ret));
            }
          } else if (OB_FALSE_IT(order_val_str.assign_ptr(order_val->get_data(), order_val->get_data_length()))) {
          } else if (order_val_str.case_compare("asc") == 0) {
            item.order_asc_ = true;
          } else if (order_val_str.case_compare("desc") == 0) {
            item.order_asc_ = false;
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "agg.terms.order, should be \"asc\" or \"desc\"");
            LOG_WARN("invalid order direction", K(ret), K(order_val_str));
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "keys other than \"field\", \"size\", \"min_doc_count\" and \"order\" in aggs.terms");
          LOG_WARN("unsupported key in aggs.terms", K(ret), K(key));
        }
      } else { // CARDINALITY
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "keys other than \"field\" in aggs.cardinality");
        LOG_WARN("unsupported cardinality parameter", K(ret), K(key));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(item.field_)) {
      ret = OB_INVALID_ARGUMENT;
      if (agg_type == ObDSLAggTermsItem::TERMS) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "aggs.terms parameter, required \"field\" is missing");
        LOG_WARN("terms aggregation requires field parameter", K(ret));
      } else {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "aggs.cardinality parameter, required \"field\" is missing");
        LOG_WARN("aggs.cardinality parameter, required \"field\" is missing", K(ret));
      }
    } else if (OB_FAIL(dsl_query_info_->agg_items_.push_back(item))) {
      LOG_WARN("fail to push back agg item", K(ret));
    }
  }
  return ret;
}

int ObDSLResolver::inject_agg_stmt_rewrites()
{
  int ret = OB_SUCCESS;
  if (!dsl_query_info_->has_dsl_aggs()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no agg items", K(ret));
  } else {
    ObRawExprFactory *expr_factory = params_.expr_factory_;
    ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(stmt_);
    ObDSLAggTermsItem &item = dsl_query_info_->agg_items_.at(0);

    if (item.agg_type_ == ObDSLAggTermsItem::TERMS) {
      // 1. GROUP BY field
      if (OB_FAIL(select_stmt->add_group_expr(item.field_))) {
        LOG_WARN("fail to add group expr", K(ret));
      }

      // 2. COUNT(*)
      ObAggFunRawExpr *count_expr = nullptr;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObRawExprUtils::build_dummy_count_expr(*expr_factory, session_info_, count_expr))) {
        LOG_WARN("fail to build count expr", K(ret));
      } else {
        item.count_expr_ = count_expr;
        if (OB_FAIL(select_stmt->add_agg_item(*count_expr))) {
          LOG_WARN("fail to add agg item", K(ret));
        }
      }

      // 3. HAVING COUNT(*) >= min_doc_count
      if (OB_SUCC(ret) && item.min_doc_count_ > 0) {
        ObConstRawExpr *min_doc_expr = nullptr;
        ObRawExpr *having_expr = nullptr;
        if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType,
                                                         item.min_doc_count_, min_doc_expr))) {
          LOG_WARN("fail to build min_doc_count expr", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(*expr_factory, T_OP_GE,
                                                                        count_expr, min_doc_expr,
                                                                        having_expr))) {
          LOG_WARN("fail to build having expr", K(ret));
        } else if (OB_FAIL(select_stmt->add_having_expr(having_expr))) {
          LOG_WARN("fail to add having expr", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        ObRawExpr *not_null_expr = nullptr;
        if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(*expr_factory, item.field_,
                                                           true /*is_not_null*/, not_null_expr))) {
          LOG_WARN("fail to build is not null expr for terms agg field", K(ret));
        } else if (OB_FAIL(select_stmt->add_having_expr(not_null_expr))) {
          LOG_WARN("fail to add not null having for terms agg field", K(ret));
        }
      }

      // 4. ORDER BY (_key or _count, with tie-breaker to match ES behavior)
      if (OB_SUCC(ret)) {
        ObRawExpr *order_expr = nullptr;
        if (item.order_by_ == ObDSLAggTermsItem::BY_KEY) {
          order_expr = item.field_;
        } else {
          order_expr = item.count_expr_;
        }
        ObOrderDirection direction = item.order_asc_ ? NULLS_LAST_ASC : NULLS_FIRST_DESC;
        OrderItem order_item(order_expr, direction);
        if (OB_FAIL(select_stmt->add_order_item(order_item))) {
          LOG_WARN("fail to add order item for aggs", K(ret));
        } else if (item.order_by_ == ObDSLAggTermsItem::BY_COUNT) {
          // ES tie-breaker: when counts are equal, sort by _key asc
          OrderItem tiebreak_item(item.field_, NULLS_LAST_ASC);
          if (OB_FAIL(select_stmt->add_order_item(tiebreak_item))) {
            LOG_WARN("fail to add tiebreak order item for aggs", K(ret));
          }
        }
      }

      // 5. LIMIT size
      if (OB_SUCC(ret) && item.size_ > 0) {
        ObConstRawExpr *limit_expr = nullptr;
        if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType,
                                                         item.size_, limit_expr))) {
          LOG_WARN("fail to build limit expr", K(ret));
        } else {
          select_stmt->set_limit_offset(limit_expr, nullptr);
        }
      }
    } else if (item.agg_type_ == ObDSLAggTermsItem::CARDINALITY) {
      // DISTINCT semantics are provided by the user-visible SELECT item
      // (COUNT(DISTINCT field)); the DSL here mainly validates the syntax.
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected agg type", K(ret), K(item.agg_type_));
    }
  }
  return ret;
}

int ObDSLResolver::check_hybrid_search_clause_compat(ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;
  bool has_dsl_order_by = false;
  bool has_dsl_limit = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_table_size(); ++i) {
    const TableItem *table_item = select_stmt->get_table_item(i);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret), K(i));
    } else if (!table_item->is_hybrid_search_table()) {
      // do nothing
    } else if (table_item->dsl_query_->has_dsl_sort() || table_item->dsl_query_->has_dsl_collapse()) {
      has_dsl_order_by = true;
      has_dsl_limit = true;
    } else {
      for (int64_t j = 0; j < table_item->dsl_query_->agg_items_.count(); ++j) {
        if (table_item->dsl_query_->agg_items_.at(j).agg_type_ == ObDSLAggTermsItem::TERMS) {
          has_dsl_order_by = true;
          has_dsl_limit = true;
          break;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (select_stmt->get_condition_size() > 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "use hybrid search with where condition");
    LOG_WARN("use hybrid search with where condition", K(ret));
  } else if (select_stmt->has_order_by() && !has_dsl_order_by) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "use hybrid search with order by");
    LOG_WARN("use hybrid search with order by", K(ret));
  } else if (select_stmt->has_limit() && !has_dsl_limit) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "use hybrid search with limit");
    LOG_WARN("use hybrid search with limit", K(ret));
  }
  return ret;
}

int ObDSLResolver::check_hybrid_search_cardinality_agg(ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_table_size(); ++i) {
    const TableItem *table_item = select_stmt->get_table_item(i);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret), K(i));
    } else if (!table_item->is_hybrid_search_table()) {
      // do nothing
    } else {
      const ObDSLAggTermsItem *cardinality_item = nullptr;
      for (int64_t j = 0; OB_ISNULL(cardinality_item) && j < table_item->dsl_query_->agg_items_.count(); ++j) {
        if (table_item->dsl_query_->agg_items_.at(j).agg_type_ == ObDSLAggTermsItem::CARDINALITY) {
          cardinality_item = &table_item->dsl_query_->agg_items_.at(j);
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(cardinality_item)) {
        const ObAggFunRawExpr *aggr_expr = nullptr;
        ObRawExpr *distinct_param = nullptr;
        if (!select_stmt->is_scala_group_by() || select_stmt->get_aggr_item_size() != 1) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "cardinality aggregation requires the aggs bucket pseudo column or COUNT(DISTINCT field) matching cardinality.field");
          LOG_WARN("invalid select shape for cardinality agg", K(ret),
                   K(select_stmt->is_scala_group_by()), K(select_stmt->get_aggr_item_size()));
        } else if (OB_ISNULL(aggr_expr = select_stmt->get_aggr_item(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("aggregate expr is null", K(ret));
        } else if (aggr_expr->get_expr_type() != T_FUN_COUNT
                   || !aggr_expr->is_param_distinct()
                   || aggr_expr->get_real_param_count() != 1) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "cardinality aggregation requires the aggs bucket pseudo column or COUNT(DISTINCT field) matching cardinality.field");
          LOG_WARN("aggregate expr does not match COUNT(DISTINCT field)", K(ret), KPC(aggr_expr));
        } else if (OB_ISNULL(distinct_param = aggr_expr->get_real_param_exprs().at(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("distinct param is null", K(ret));
        } else if (OB_ISNULL(cardinality_item->field_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cardinality field is null", K(ret));
        } else if (!distinct_param->same_as(*cardinality_item->field_)) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "cardinality aggregation requires the aggs bucket pseudo column or COUNT(DISTINCT field) matching cardinality.field");
          LOG_WARN("distinct param does not match cardinality field", K(ret),
                   KPC(distinct_param), KPC(cardinality_item->field_));
        }
      }
    }
  }
  return ret;
}

int ObDSLResolver::check_hybrid_search_stmt(ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!select_stmt->is_hybrid_search()) {
    // do nothing
  } else if (OB_FAIL(refresh_result_modes(select_stmt))) {
    LOG_WARN("failed to refresh hybrid search result modes", K(ret));
  } else if (OB_FAIL(check_hybrid_search_clause_compat(select_stmt))) {
    LOG_WARN("hybrid search clause compatibility check failed", K(ret));
  } else if (OB_FAIL(check_hybrid_search_cardinality_agg(select_stmt))) {
    LOG_WARN("hybrid search cardinality aggregation check failed", K(ret));
  }
  return ret;
}

bool ObDSLResolver::is_hybrid_search_count_star_stmt(const ObSelectStmt &select_stmt,
                                                     const ObAggFunRawExpr *&aggr_expr)
{
  bool is_count_star = false;
  aggr_expr = nullptr;
  if (select_stmt.get_select_item_size() == 1) {
    const SelectItem &select_item = select_stmt.get_select_item(0);
    aggr_expr = OB_NOT_NULL(select_item.expr_) && select_item.expr_->is_aggr_expr()
                    ? static_cast<const ObAggFunRawExpr *>(select_item.expr_)
                    : nullptr;
    is_count_star = OB_NOT_NULL(aggr_expr)
                    && aggr_expr->get_expr_type() == T_FUN_COUNT
                    && !aggr_expr->is_param_distinct()
                    && aggr_expr->get_real_param_count() == 0;
  }
  return is_count_star;
}

int ObDSLResolver::rewrite_size_to_rank_window_for_agg_query(ObDSLQueryInfo &dsl_query_info)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *min_score_expr = nullptr;
  ObConstRawExpr *from_expr = nullptr;
  double min_score_value = 0.0;

  if ((dsl_query_info.result_mode_ == ObDSLResultMode::COUNT_AGG ||
       dsl_query_info.result_mode_ == ObDSLResultMode::BUCKET_AGG) &&
       dsl_query_info.has_dsl_rank()) {
    if (OB_ISNULL(min_score_expr = static_cast<ObConstRawExpr *>(dsl_query_info.min_score_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("min score expr is null", K(ret));
    } else if (OB_FAIL(min_score_expr->get_value().get_double(min_score_value))) {
      LOG_WARN("failed to get double value from min score expr", K(ret));
    } else if (min_score_value > 0) {
      if (OB_ISNULL(dsl_query_info.rank_info_.window_size_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rank window size expr is null", K(ret));
      } else if (OB_ISNULL(from_expr = static_cast<ObConstRawExpr *>(dsl_query_info.from_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("from expr is null", K(ret));
      } else {
        dsl_query_info.size_ = dsl_query_info.rank_info_.window_size_;
        from_expr->get_value().set_int(0);
      }
    }
  }
  return ret;
}

int ObDSLResolver::ignore_count_star_dsl_rewrites(ObSelectStmt &select_stmt,
                                                  ObDSLQueryInfo &dsl_query_info)
{
  int ret = OB_SUCCESS;
  const ObAggFunRawExpr *const_aggr_expr = nullptr;
  ObAggFunRawExpr *aggr_expr = nullptr;
  if (!ObDSLResolver::is_hybrid_search_count_star_stmt(select_stmt, const_aggr_expr)) {
    // do nothing
  } else if (OB_ISNULL(aggr_expr = const_cast<ObAggFunRawExpr *>(const_aggr_expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggregate expr is null", K(ret));
  } else {
    // select count(*) means "count raw query/knn hits"; ignore all DSL-level
    // reshaping operators such as aggs, sort, collapse and pagination.
    select_stmt.clear_aggr_item();
    if (OB_FAIL(select_stmt.add_agg_item(*aggr_expr))) {
      LOG_WARN("failed to restore outer count(*) aggregate", K(ret));
    } else {
      select_stmt.get_order_items().reset();
      select_stmt.get_group_exprs().reset();
      select_stmt.get_having_exprs().reset();
      select_stmt.set_limit_offset(nullptr, nullptr);
      select_stmt.get_window_func_exprs().reset();
      select_stmt.get_qualify_filters().reset();

      dsl_query_info.sort_items_.reset();
      dsl_query_info.collapse_info_.win_func_expr_ = nullptr;
      dsl_query_info.collapse_info_.qualify_expr_ = nullptr;
      dsl_query_info.agg_items_.reset();
      dsl_query_info.result_mode_ = ObDSLResultMode::COUNT_AGG;
      set_track_score(&dsl_query_info);
      if (OB_FAIL(refresh_output_score_after_select_resolved(&select_stmt, &dsl_query_info))) {
        LOG_WARN("failed to refresh output score after count(*) rewrites", K(ret));
      }
    }
  }
  return ret;
}

int ObDSLResolver::register_collapse_exprs(ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && select_stmt->is_hybrid_search() && i < select_stmt->get_table_size(); ++i) {
    const TableItem *table_item = select_stmt->get_table_item(i);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret), K(i));
    } else if (!table_item->is_hybrid_search_table()) {
      // do nothing
    } else if (!table_item->dsl_query_->has_dsl_collapse() ||
               OB_ISNULL(table_item->dsl_query_->collapse_info_.win_func_expr_)) {
      // do nothing
    } else if (OB_FAIL(ObExpandAggregateUtils::add_win_expr(
                   select_stmt->get_window_func_exprs(),
                   table_item->dsl_query_->collapse_info_.win_func_expr_))) {
      LOG_WARN("fail to register collapse window expr", K(ret));
    } else if (OB_NOT_NULL(table_item->dsl_query_->collapse_info_.qualify_expr_) &&
               OB_FAIL(select_stmt->get_qualify_filters().push_back(table_item->dsl_query_->collapse_info_.qualify_expr_))) {
      LOG_WARN("fail to add qualify filter for collapse", K(ret));
    }
  }
  return ret;
}

int ObDSLResolver::refresh_result_modes(ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_table_size(); ++i) {
    const TableItem *table_item = select_stmt->get_table_item(i);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret), K(i));
    } else if (!table_item->is_hybrid_search_table()) {
      // do nothing
    } else if (OB_FAIL(ObDSLResolver::refresh_result_mode_after_select_resolved(select_stmt, table_item->dsl_query_))) {
      LOG_WARN("failed to refresh result mode after select resolved", K(ret));
    } else if (OB_FAIL(ObDSLResolver::ignore_count_star_dsl_rewrites(*select_stmt, *table_item->dsl_query_))) {
      LOG_WARN("failed to ignore dsl rewrites for count(*)", K(ret));
    } else if (OB_FAIL(ObDSLResolver::rewrite_size_to_rank_window_for_agg_query(*table_item->dsl_query_))) {
      LOG_WARN("failed to rewrite size for aggregation query with rank and min_score", K(ret));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
