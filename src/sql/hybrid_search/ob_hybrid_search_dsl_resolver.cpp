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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/hybrid_search/ob_hybrid_search_dsl_resolver.h"
#include "sql/hybrid_search/ob_fulltext_search_query.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

namespace oceanbase
{
namespace sql
{

const ObString ObDSLResolver::FTS_SCORE_NAME("__fts_score");
const ObString ObDSLResolver::VS_SCORE_PREFIX("__vs_score_");
const int64_t ObDSLResolver::KNN_K_VALUE_MAX;

int ObDSLResolver::resolve_hybrid_search_score_column_ref_expr(
    const TableItem &table_item,
    const ObQualifiedName &q_name,
    ObDMLStmt &stmt,
    ObRawExpr *&real_ref_expr)
{
  int ret = OB_SUCCESS;

  if (!table_item.is_hybrid_search_table()
      || 0 != q_name.col_name_.case_compare(OB_HYBRID_SEARCH_SCORE_COLUMN_NAME)) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_WARN("not a hybrid search score column", K(ret), K(q_name));
  } else {
    const ObDSLQueryInfo *dsl_query = table_item.dsl_query_;
    ObOpPseudoColumnRawExpr *score_expr = nullptr;
    if (OB_ISNULL(dsl_query)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hybrid search table has null dsl_query_", K(ret), K(table_item));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(score_expr) && i < dsl_query->score_cols_.count(); ++i) {
        ObOpPseudoColumnRawExpr *tmp = dsl_query->score_cols_.at(i);
        if (OB_ISNULL(tmp)) {
          // skip
        } else if (OB_NOT_NULL(tmp->get_name())
                   && 0 == ObString::make_string(tmp->get_name()).case_compare(OB_HYBRID_SEARCH_SCORE_COLUMN_NAME)) {
          score_expr = tmp;
        }
      }
      if (OB_ISNULL(score_expr)) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("hybrid search score expr not found", K(ret), K(q_name));
      } else {
        const int64_t rel_id = stmt.get_table_bit_index(table_item.table_id_);
        if (OB_FAIL(score_expr->add_relation_id(rel_id))) {
          LOG_WARN("failed to add relation id for hybrid search score", K(ret), K(rel_id));
        } else {
          real_ref_expr = score_expr;
        }
      }
    }
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
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_WARN("not a hybrid search table", K(ret), K(table_item));
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
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param, invalid param", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src.from_, from_))) {
    LOG_WARN("failed to copy from expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src.size_, size_))) {
    LOG_WARN("failed to copy size expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src.min_score_, min_score_))) {
    LOG_WARN("failed to copy min score expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src.rowkey_cols_, rowkey_cols_))) {
    LOG_WARN("failed to copy rowkey cols", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src.dsl_cols, dsl_cols))) {
    LOG_WARN("failed to copy scalar query cols", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src.score_cols_, score_cols_))) {
    LOG_WARN("failed to copy score cols", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src.dsl_exprs_, dsl_exprs_))) {
    LOG_WARN("failed to copy dsl exprs", K(ret));
  } else if (OB_FALSE_IT(rank_info_.method_ = src.rank_info_.method_)) {
  } else if (OB_FAIL(expr_copier.copy(src.rank_info_.window_size_, rank_info_.window_size_))) {
    LOG_WARN("failed to copy window_size expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(src.rank_info_.rank_const_, rank_info_.rank_const_))) {
    LOG_WARN("failed to copy rank_const expr", K(ret));
  } else if (OB_FAIL(ob_write_string(*allocator, src.raw_dsl_param_str_, raw_dsl_param_str_))) {
    LOG_WARN("failed to copy raw dsl param string", K(ret));
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
    dst->must_cnt_ = src->must_cnt_;
    dst->should_cnt_ = src->should_cnt_;
    dst->filter_cnt_ = src->filter_cnt_;
    dst->must_not_cnt_ = src->must_not_cnt_;
    dst->msm_ = src->msm_;
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
  } else if (OB_FAIL(ObDSLKnnQuery::create(*allocator, dst, src->outer_query_type_))) {
    LOG_WARN("failed to create knn query", K(ret));
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
  // no need to create these exprs for top k query, they would be set when resolving dsl
  if (!is_top_k_query) {
    ObConstRawExpr *from_expr = nullptr;
    ObConstRawExpr *size_expr = nullptr;
    ObConstRawExpr *min_score_expr = nullptr;
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, ObDSLResolver::FROM_DEFAULT, from_expr))) {
      LOG_WARN("failed to create from const expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, ObDSLResolver::SIZE_DEFAULT, size_expr))) {
      LOG_WARN("failed to create size const expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_double_expr(expr_factory, ObDoubleType, ObDSLResolver::MIN_SCORE_DEFAULT, min_score_expr))) {
      LOG_WARN("failed to create min score const expr", K(ret));
    } else {
      from_ = from_expr;
      size_ = size_expr;
      min_score_ = min_score_expr;
      rank_info_.method_ = ObFusionMethod::WEIGHT_SUM;
      rank_info_.window_size_ = size_expr;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObRawExprUtils::build_const_double_expr(expr_factory, ObDoubleType, 1.0, one_const_expr))) {
    LOG_WARN("failed to create default boost const expr", K(ret));
  } else {
    one_const_expr_ = one_const_expr;
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
    if (OB_FAIL(get_fulltext_index_schema(col_name, index_schema))) {
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

int ObDSLResolver::get_dist_algo_type(ObColumnRefRawExpr *field_expr, ObVectorIndexDistAlgorithm &algo_type)
{
  int ret = OB_SUCCESS;
  ObColumnIndexInfo *idx_info = nullptr;
  if (!col_idx_map_.created()) {
    // do nothing
  } else if (OB_FAIL(col_idx_map_.get_refactored(field_expr->get_column_name(), idx_info))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get col idx info", K(ret));
    }
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
  index_schema = nullptr;
  if (!col_idx_map_.created()) {
  } else if (OB_FAIL(col_idx_map_.get_refactored(col_name, idx_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get col idx info", K(col_name), K(ret));
    }
  } else if (OB_ISNULL(idx_info->index_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schema is null", K(ret), K(col_name));
  } else if (!share::schema::is_fts_index_aux(idx_info->index_schema_->get_index_type())) {
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

int ObDSLResolver::get_user_column_expr(const ObString &col_name, ObColumnRefRawExpr *&col_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  ColumnItem *existing_col_item = nullptr;
  const ObColumnSchemaV2 *col_schema = nullptr;
  if (col_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column name is empty", K(ret));
  } else if (OB_NOT_NULL(existing_col_item = stmt_->get_column_item(table_item_.table_id_, col_name))) {
    col_expr = existing_col_item->expr_;
  } else if (OB_FAIL(col_schema_map_.get_refactored(col_name, col_schema))) {
    // OB_HASH_NOT_EXIST would not be logged here and should be handled in the caller
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get column schema from hash map", K(ret), K(col_name));
    }
  } else if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, *col_schema, session_info_, col_expr))) {
    LOG_WARN("failed to build column expr", K(ret), K(col_schema->get_column_id()), K(col_name));
  }

  if (OB_FAIL(ret)) {
  } else if (col_expr->is_virtual_generated_column()) {
    // keep col_expr not null to let the caller handle the error
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "virtual generated column in dsl");
    LOG_WARN("virtual generated column is not supported", K(ret), K(col_name));
  } else {
    setup_column_expr_attr(col_expr);
    if (OB_NOT_NULL(existing_col_item)) {
      // do nothing
    } else {
      ColumnItem column_item;
      column_item.expr_ = col_expr;
      column_item.table_id_ = table_item_.table_id_;
      column_item.column_id_ = col_expr->get_column_id();
      column_item.column_name_ = col_expr->get_column_name();
      column_item.base_tid_ = table_item_.ref_id_;
      column_item.base_cid_ = column_item.column_id_;
      column_item.is_geo_ = col_schema->is_geometry();
      column_item.set_default_value(col_schema->get_cur_default_value());
      if (OB_FAIL(stmt_->add_column_item(column_item))) {
        LOG_WARN("fail to add column item to stmt", K(ret), K(col_name));
      }
    }
  }
  return ret;
}

int ObDSLResolver::init_bool_info(ObIJsonBase &req_node, int32_t &msm, ObConstRawExpr *&boost_expr)
{
  int ret = OB_SUCCESS;
  bool has_msm_key = false;
  bool has_must = false;
  bool has_filter = false;
  bool has_should = false;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
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
      } else if (key.case_compare("minimum_should_match") == 0) {
        has_msm_key = true;
        if (OB_FAIL(resolve_minimum_should_match(*sub_node, msm))) {
          LOG_WARN("fail to resolve minimum_should_match", K(ret));
        }
      } else if (key.case_compare("boost") == 0) {
        if (OB_FAIL(resolve_boost(*sub_node, boost_expr, QUERY_ITEM_BOOL, QUERY_ITEM_UNKNOWN))) {
          LOG_WARN("fail to resolve boost", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && !has_msm_key) {
      if (has_should && !has_must && !has_filter) {
        msm = 1;
      } else {
        msm = 0;
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
            for (int64_t k = 0; OB_SUCC(ret) && k < cascaded_column_ids.count(); k++) {
              const ObColumnSchemaV2 *cascaded_column = nullptr;
              ObString column_name;
              ObColumnIndexInfo *existing_idx_info = nullptr;
              if (OB_ISNULL(cascaded_column = table_schema_->get_column_schema(cascaded_column_ids.at(k)))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected cascaded column", K(ret));
              } else if (cascaded_column->is_hidden() ||
                         cascaded_column->is_unused() ||
                         cascaded_column->is_invisible_column() ||
                         cascaded_column->is_shadow_column()) {
                // do nothing
              } else if (OB_FALSE_IT(column_name = cascaded_column->get_column_name_str())) {
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
  ObJsonNodeType args_type = ObJsonNodeType::J_MAX_TYPE;
  ObIJsonBase *param_node = nullptr;
  ObColumnRefRawExpr *col_expr = nullptr;
  ObRawExpr *args_expr = nullptr;
  ObSysFunRawExpr *array_expr = nullptr;
  ObDSLScalarQuery *array_query = nullptr;
  if (ObDSLQuery::check_need_cal_score_in_bool(outer_query_type, parent_query)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "array query in must/should clause");
    LOG_WARN("array query cannot be scored or exist in must/should clause", K(ret), K(query_type));
  } else if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("array query should be object", K(ret));
  } else if (req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("array expr should have exactly one element", K(ret));
  } else if (OB_FAIL(req_node.get_object_value(0, col_name, param_node))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (OB_FALSE_IT(args_type = (query_type == QUERY_ITEM_ARRAY_CONTAINS) ? param_node->json_type() : ObJsonNodeType::J_ARRAY)) {
  } else if (OB_FAIL(resolve_const(*param_node, args_expr, args_type, query_type))) {
    LOG_WARN("fail to resolve arg expr", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (query_type == QUERY_ITEM_ARRAY_CONTAINS) {
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
    LOG_WARN("unsupported array expr type", K(ret), K(query_type));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_user_column_expr(col_name, col_expr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "array expression, column not exists");
    }
    LOG_WARN("fail to get user column expr", K(ret), K(col_name));
  } else if (!col_expr->get_result_type().is_collection_sql_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "array expression, column is not an array type");
    LOG_WARN("array expression for non-array column", K(ret), K(col_name));
  } else if (OB_FAIL(expr_factory->create_raw_expr(expr_type, array_expr))) {
    LOG_WARN("fail to create array expr", K(ret));
  } else if (OB_FALSE_IT(array_expr->set_func_name(expr_name))) {
  } else if (OB_FAIL(array_expr->set_param_exprs(col_expr, args_expr))) {
    LOG_WARN("fail to set param exprs for array expr", K(ret));
  } else if (OB_FAIL(ObDSLScalarQuery::create(*allocator_, array_query, query_type, outer_query_type, parent_query))) {
    LOG_WARN("fail to create array query", K(ret));
  } else {
    array_query->field_ = col_expr;
    array_query->scalar_expr_ = array_expr;
    array_query->boost_ = setup_boost(nullptr);
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
  const char *syntaxerr = nullptr;
  uint64_t err_offset = 0;
  ObString req_str;
  ParseNode *raw_text = parse_tree.children_[0];
  uint32_t parse_flag = ObJsonParser::JSN_RELAXED_FLAG | ObJsonParser::JSN_UNIQUE_FLAG;
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
      } else {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_USER_ERROR(OB_ERR_PARSER_SYNTAX, "invalid search param, unsupported key");
        LOG_WARN("invalid search param", K(ret), K(key));
      }
    }
    if (OB_SUCC(ret) && dsl_query_info_->queries_.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid search param, must contain at least one query");
      LOG_WARN("invalid search param, must contain at least one query", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(construct_rowkey_columns())) {
    LOG_WARN("fail to construct rowkey columns", K(ret));
  } else if (OB_FAIL(construct_score_columns())) {
    LOG_WARN("fail to construct score columns", K(ret));
  } else if (OB_FAIL(collect_exprs())) {
    LOG_WARN("fail to collect exprs", K(ret));
  } else if (OB_FAIL(formalize_exprs())) {
    LOG_WARN("fail to formalize exprs", K(ret));
  }
  return ret;
}

int ObDSLResolver::resolve_boost(ObIJsonBase &req_node, ObConstRawExpr *&boost_expr, ObEsQueryItem query_type, ObEsQueryItem outer_query_type)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = nullptr;
  double boost_value = 0.0;
  if (OB_FAIL(resolve_const(req_node, expr, ObJsonNodeType::J_DOUBLE))) {
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
  int32_t min_should_match = ObDSLMatchQuery::DEFAULT_MINIMUM_SHOULD_MATCH;
  ObMatchOperator match_operator = ObDSLMatchQuery::DEFAULT_OPERATOR;
  ObCollationType collation_type = CS_TYPE_INVALID;
  ObDSLMatchQuery *match_query = nullptr;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("match expr should be object", K(ret));
  } else if (req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "match query, should contain exactly one element");
    LOG_WARN("match expr should have exactly one element", K(ret));
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
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "boost in match query");
          LOG_WARN("fail to parse boost value", K(ret));
        }
      } else if (key.case_compare("minimum_should_match") == 0) {
        if (OB_FAIL(resolve_minimum_should_match(*sub_node, min_should_match))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "minimum_should_match in match query");
          LOG_WARN("fail to resolve minimum should match", K(ret));
        }
      } else if (key.case_compare("operator") == 0) {
        if (OB_FAIL(resolve_query_string_operator(*sub_node, match_operator))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "operator in match query");
          LOG_WARN("fail to resolve query string operator", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Parameter in match query");
        LOG_WARN("unsupported key in match query", K(ret), K(key));
      }
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("value should be string or object", K(ret), K(col_para->json_type()));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(query_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "match query, required param \"query\" is missed");
    LOG_WARN("missing query node", K(ret));
  } else if (OB_FALSE_IT(collation_type = col_expr->get_collation_type())) {
  } else if (OB_FAIL(resolve_query_string_expr(ObString(query_node->get_data_length(), query_node->get_data()), collation_type, query_expr))) {
    LOG_WARN("fail to construct string expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
      *expr_factory, ObIntType, static_cast<int64_t>(min_should_match), min_should_match_expr))) {
    LOG_WARN("fail to build const int expr for minimum should match", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
      *expr_factory, ObIntType, static_cast<int64_t>(match_operator), operator_expr))) {
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
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("match expr should be object", K(ret));
  } else if (req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "match_phrase query, should contain exactly one element");
    LOG_WARN("match phrase expr should have exactly one element", K(ret));
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
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "boost in match_phrase query");
          LOG_WARN("fail to parse boost value", K(ret));
        }
      } else if (key.case_compare("slop") == 0) {
        if (OB_FAIL(resolve_slop(*sub_node, slop))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "slop in match_phrase query");
          LOG_WARN("fail to resolve slop", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Parameter in match_phrase query");
        LOG_WARN("unsupported key in match_phrase query", K(ret), K(key));
      }
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("value should be string or object", K(ret), K(col_para->json_type()));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(query_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "match_phrase query, required param \"query\" is missed");
    LOG_WARN("missing query node", K(ret));
  } else if (OB_FALSE_IT(collation_type = col_expr->get_collation_type())) {
  } else if (OB_FAIL(resolve_query_string_expr(ObString(query_node->get_data_length(), query_node->get_data()), collation_type, query_expr))) {
    LOG_WARN("fail to construct string expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
      *expr_factory, ObIntType, static_cast<int64_t>(slop), slop_expr))) {
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
  ObSEArray<ObDSLQuery*, 4, ModulePageAllocator, true> filter_queries;
  ObConstRawExpr *boost_expr = nullptr;
  ObDSLKnnQuery::SearchOption *search_option = nullptr;
  ObDSLKnnQuery *knn_query = nullptr;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpected json type", K(ret), K(req_node.json_type()));
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
        ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
        LOG_WARN("unexpected json type", K(ret), K(sub_node->json_type()));
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
      int64_t k_value = 0;
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
    } else if (key.case_compare("boost") == 0) {
      if (OB_FAIL(resolve_boost(*sub_node, boost_expr, QUERY_ITEM_KNN, QUERY_ITEM_UNKNOWN))) {
        LOG_WARN("fail to resolve boost", K(ret));
      }
    } else if (key.case_compare("similarity") == 0) {
      double similarity = 0.0;
      if (OB_LIKELY(sub_node->is_json_number(sub_node->json_type()))) {
        if (OB_FAIL(sub_node->to_double(similarity))) {
          LOG_WARN("fail to get double value from similarity", K(ret));
        }
      } else if (sub_node->json_type() == ObJsonNodeType::J_STRING) {
        ObString similarity_str = ObString(sub_node->get_data_length(), sub_node->get_data());
        if (OB_FAIL(trim_strtod(similarity_str, similarity))) {
          LOG_WARN("fail to convert string to double", K(ret));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("similarity should be number or string type", K(ret), K(sub_node->json_type()));
      }
      if (OB_FAIL(ret)) {
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
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "knn query, required params \"field\", \"query_vector\", \"k\" are missed");
    LOG_WARN("knn required params are missed", K(ret), K(required_params.begin()->first));
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
  int32_t msm = 1;
  ObConstRawExpr *boost_expr = nullptr;
  ObDSLBoolQuery *bool_query = nullptr;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else if (OB_FALSE_IT(count = req_node.element_count())) {
  } else if (count == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "bool query, it cannot be empty");
    LOG_WARN("empty bool query", K(ret));
  } else if (OB_FAIL(init_bool_info(req_node, msm, boost_expr))) {
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
    } else if (key.case_compare("minimum_should_match") == 0) {
      // do nothing
    } else if (key.case_compare("boost") == 0) {
      // do nothing
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid key in bool query");
      LOG_WARN("not supported syntax in query", K(ret), K(key));
    }
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
    bool_query->msm_ = msm;
    bool_query->boost_ = setup_boost(boost_expr);
    // score is true only if there is at least one item which really needs to calculate score
    bool_query->need_cal_score_ = bool_query->need_cal_score_ && score_cnt > 0;
    query = bool_query;
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
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
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
// json_type \ target_type | J_STRING | J_INT | J_UINT |J_DOUBLE | J_ARRAY | J_OBJECT | J_BOOLEAN
// ------------------------|----------|-------|--------|---------|---------|----------|----------
// J_STRING                |   true   | true  |  true  |  true   |  false  |   true   |   true
// J_INT                   |   false  | true  |  true  |  true   |  false  |   false  |   false
// J_UINT                  |   false  | true  |  true  |  true   |  false  |   false  |   false
// J_DOUBLE                |   false  | true  |  true  |  true   |  false  |   false  |   false
// J_ARRAY                 |   true   | false |  false |  false  |  true   |   true   |   false
// J_OBJECT                |   true   | false |  false |  false  |  false  |   true   |   false
// J_BOOLEAN               |   false  | false |  false |  false  |  false  |   false  |   true
int ObDSLResolver::resolve_const(ObIJsonBase &req_node, ObRawExpr *&expr, ObJsonNodeType target_type, ObEsQueryItem query_type/*=QUERY_ITEM_UNKNOWN*/)
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
  } else if (target_type == ObJsonNodeType::J_INT ||
             target_type == ObJsonNodeType::J_UINT ||
             target_type == ObJsonNodeType::J_DOUBLE) {
    can_cast = (req_node.is_json_number(json_type) || (json_type == ObJsonNodeType::J_STRING));
  } else if (target_type == ObJsonNodeType::J_ARRAY) {
    can_cast = (json_type == ObJsonNodeType::J_ARRAY);
  } else if (target_type == ObJsonNodeType::J_OBJECT) {
    can_cast = (json_type == ObJsonNodeType::J_OBJECT);
  } else if (target_type == ObJsonNodeType::J_BOOLEAN) {
    can_cast = (json_type == ObJsonNodeType::J_BOOLEAN || json_type == ObJsonNodeType::J_STRING);
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpected target type", K(ret), K(target_type));
  }
  if (OB_FAIL(ret)) {
  } else if (!can_cast) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("type conversion not allowed", K(ret), K(json_type), K(target_type));
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
    double num_value = 0.0;
    if (json_type == ObJsonNodeType::J_STRING) {
      ObString str_val = ObString(req_node.get_data_length(), req_node.get_data());
      if (OB_FAIL(trim_strtod(str_val, num_value))) {
        LOG_WARN("fail to convert string to double", K(ret), K(str_val));
      }
    } else if (json_type == ObJsonNodeType::J_INT) {
      num_value = static_cast<double>(req_node.get_int());
    } else if (json_type == ObJsonNodeType::J_UINT) {
      num_value = static_cast<double>(req_node.get_uint());
    } else if (json_type == ObJsonNodeType::J_DOUBLE) {
      num_value = req_node.get_double();
    }
    if (OB_FAIL(ret)) {
    } else if (target_type == ObJsonNodeType::J_INT || target_type == ObJsonNodeType::J_UINT) {
      if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, static_cast<int64_t>(num_value), const_expr))) {
        LOG_WARN("fail to create const int expr", K(ret));
      } else {
        expr = const_expr;
      }
    } else {
      if (num_value == 1.0) {
        expr = dsl_query_info_->one_const_expr_;
      } else if (OB_FAIL(ObRawExprUtils::build_const_double_expr(*expr_factory, ObDoubleType, num_value, const_expr))) {
        LOG_WARN("fail to create const double expr", K(ret), K(target_type));
      } else {
        expr = const_expr;
      }
    }
  } else if (target_type == ObJsonNodeType::J_ARRAY) {
    ObSysFunRawExpr *array_expr = nullptr;
    int64_t count = req_node.element_count();
    if (count == 0) {
      ret = OB_INVALID_ARGUMENT;
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
        } else if (OB_FALSE_IT(elem_json_type = elem_node->json_type())) {
        } else if (OB_FAIL(resolve_const(*elem_node, elem_expr, elem_json_type, query_type))) {
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
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "boolean value, must be 'true' or 'false' when it is a string");
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

int ObDSLResolver::resolve_default_params(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  const ObString size_key = "size";
  const ObString from_key = "from";
  const ObString rank_key = "rank";
  const ObString min_score_key = "min_score";
  ObIJsonBase *size_node = nullptr;
  ObIJsonBase *from_node = nullptr;
  ObIJsonBase *rank_node = nullptr;
  ObIJsonBase *min_score_node = nullptr;
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
  }

  bool has_rank_window_size = false;
  // 1) Set size and rank_window_size (defaults where not provided).
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(dsl_query_info_->size_) && OB_ISNULL(dsl_query_info_->rank_info_.window_size_)) {
      ObConstRawExpr *size_expr = nullptr;
      if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, SIZE_DEFAULT, size_expr))) {
        LOG_WARN("fail to create const expr for default size", K(ret));
      } else if (OB_FALSE_IT(dsl_query_info_->size_ = size_expr)) {
      } else if (OB_FAIL(set_default_rank_window_size())) {
        LOG_WARN("fail to set default rank_window_size", K(ret));
      }
    } else if (OB_ISNULL(dsl_query_info_->size_) && OB_NOT_NULL(dsl_query_info_->rank_info_.window_size_)) {
      dsl_query_info_->size_ = dsl_query_info_->rank_info_.window_size_;
      has_rank_window_size = true;
    } else if (OB_NOT_NULL(dsl_query_info_->size_) && OB_ISNULL(dsl_query_info_->rank_info_.window_size_)) {
      if (OB_FAIL(set_default_rank_window_size())) {
        LOG_WARN("fail to set default rank_window_size", K(ret));
      }
    }
  }

  // 2) Get from/size/rank_window_size values and validate them.
  if (OB_SUCC(ret)) {
    int64_t from_value = 0;
    int64_t size_value = 0;
    int64_t rank_window_size_value = 0;
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
    } else if (from_value < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "from, from value should be a non-negative integer");
      LOG_WARN("from value should be a non-negative integer", K(ret), K(from_value));
    } else if (size_value < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "size, size value should be a non-negative integer");
      LOG_WARN("size value should be a non-negative integer", K(ret), K(size_value));
    } else if (rank_window_size_value < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "rank_window_size, rank_window_size value should be a non-negative integer");
      LOG_WARN("rank_window_size value should be a non-negative integer", K(ret), K(rank_window_size_value));
    } else if (rank_window_size_value < size_value) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "rank_window_size, rank_window_size value should be greater than or equal to size value");
      LOG_WARN("rank_window_size value should be greater than or equal to size value",
               K(ret), K(rank_window_size_value), K(size_value));
    } else if (has_rank_window_size && rank_window_size_value > SIZE_VALUE_MAX) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "rank_window_size, rank_window_size value should be in range [0, 10000]");
      LOG_WARN("rank_window_size value should be in range [0, 10000]", K(ret), K(rank_window_size_value));
    } else if (from_value + size_value > SIZE_VALUE_MAX) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "from or size, from + size value should be in range [0, 10000]");
      LOG_WARN("from + size value should be in range [0, 10000]", K(ret), K(from_value), K(size_value));
    }
  }
  return ret;
}

int ObDSLResolver::resolve_field(ObIJsonBase &field_node, ObColumnRefRawExpr *&col_expr, ObConstRawExpr *&boost_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  if (field_node.json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpected json type", K(ret), K(field_node.json_type()));
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
  ObRawExpr *path_expr = nullptr;
  ObRawExpr *target_expr = nullptr;
  ObSysFunRawExpr *json_expr = nullptr;
  ObSysFunRawExpr *json_extract_expr = nullptr;
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
  } else if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("json query should be object", K(ret));
  } else if (req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json expr should have exactly one element", K(ret));
  } else if (OB_FAIL(req_node.get_object_value(0, col_name, param_node))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (param_node->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("value should be object", K(ret));
  } else {
    for (uint64_t i = 0; OB_SUCC(ret) && i < param_node->element_count(); i++) {
      ObString key;
      ObIJsonBase *sub_node = nullptr;
      if (OB_FAIL(param_node->get_object_value(i, key, sub_node))) {
        LOG_WARN("fail to get value", K(ret), K(i));
      } else if (key.case_compare("candidate") == 0) {
        ObJsonNodeType candidate_type = ObJsonNodeType::J_STRING;
        if (query_type == QUERY_ITEM_JSON_MEMBER_OF) {
          candidate_type = sub_node->json_type();
        }
        if (OB_FAIL(resolve_const(*sub_node, candidate_expr, candidate_type, query_type))) {
          LOG_WARN("fail to resolve candidate", K(ret));
        }
      } else if (key.case_compare("path") == 0) {
        if (sub_node->json_type() != ObJsonNodeType::J_STRING) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("path should be string type", K(ret), K(sub_node->json_type()));
        } else if (OB_FALSE_IT(path_str.assign_ptr(sub_node->get_data(), sub_node->get_data_length()))) {
        } else if (OB_FAIL(common::ob_strip_space(*allocator_, path_str, path_str))) {
          LOG_WARN("fail to remove whitespace", K(ret), K(path_str));
        } else if (query_type == QUERY_ITEM_JSON_MEMBER_OF && path_str.compare_equal("$")) {
          // for json member of, if path is "$", it means the root node is array, so no need to designate a path
        } else if (OB_FAIL(construct_string_expr(path_str, path_expr))) {
          LOG_WARN("fail to create path expr", K(ret), K(path_str));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid key in json query");
        LOG_WARN("unsupported key in json query", K(ret), K(key));
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
  } else if (OB_FAIL(expr_factory->create_raw_expr(expr_type, json_expr))) {
    LOG_WARN("fail to create json expr", K(ret));
  } else if (OB_FALSE_IT(json_expr->set_func_name(expr_name))) {
  } else if (OB_ISNULL(path_expr)) {
    target_expr = col_expr;
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SYS_JSON_EXTRACT, json_extract_expr))) {
    LOG_WARN("fail to create json extract expr", K(ret));
  } else if (OB_FALSE_IT(json_extract_expr->set_func_name(N_JSON_EXTRACT))) {
  } else if (OB_FAIL(json_extract_expr->set_param_exprs(col_expr, path_expr))) {
    LOG_WARN("fail to set param exprs for json_extract", K(ret));
  } else {
    target_expr = json_extract_expr;
  }
  if (OB_FAIL(ret)) {
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
    json_query->boost_ = setup_boost(nullptr);
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
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("search options should be object", K(ret));
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
      if (sub_node->json_type() != ObJsonNodeType::J_INT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("ef_search should be int type", K(ret), K(sub_node->json_type()));
      } else if (OB_FAIL(sub_node->to_int(ef_search))) {
        LOG_WARN("fail to get int value from ef_search", K(ret));
      } else if (ef_search < 1 || ef_search > 1000) {
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
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported key in search_option", K(ret), K(key));
    }
  }
  return ret;
}

int ObDSLResolver::resolve_single_term(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type)
{
  int ret = OB_SUCCESS;
  ObString key;
  ObIJsonBase *sub_node = nullptr;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else if (req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("single term must only contain one term", K(ret));
  } else if (OB_FAIL(req_node.get_object_value(0, key, sub_node))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (key.case_compare("bool") == 0) {
    if (OB_FAIL(resolve_bool(*sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve bool", K(ret));
    }
  } else if (key.case_compare("range") == 0) {
    if (OB_FAIL(resolve_range(*sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve range", K(ret));
    }
  } else if (key.case_compare("term") == 0) {
    if (OB_FAIL(resolve_term(*sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve term", K(ret));
    }
  } else if (key.case_compare("terms") == 0) {
    if (OB_FAIL(resolve_terms(*sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve terms", K(ret));
    }
  } else if (key.case_compare("match") == 0) {
    if (OB_FAIL(resolve_match(*sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve match", K(ret));
    }
  } else if (key.case_compare("match_phrase") == 0) {
    if (OB_FAIL(resolve_match_phrase(*sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve match phrase", K(ret));
    }
  } else if (key.case_compare("multi_match") == 0) {
    if (OB_FAIL(resolve_multi_match(*sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve multi match", K(ret));
    }
  } else if (key.case_compare("query_string") == 0) {
    if (OB_FAIL(resolve_query_string(*sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve query string", K(ret));
    }
  } else if (key.case_compare("array_contains") == 0) {
    if (OB_FAIL(resolve_array_contains(*sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve array contains", K(ret));
    }
  } else if (key.case_compare("array_contains_all") == 0) {
    if (OB_FAIL(resolve_array_contains_all(*sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve array contains_all", K(ret));
    }
  } else if (key.case_compare("array_overlaps") == 0) {
    if (OB_FAIL(resolve_array_overlaps(*sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve array overlaps", K(ret));
    }
  } else if (key.case_compare("json_contains") == 0) {
    if (OB_FAIL(resolve_json_contains(*sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve json contains", K(ret));
    }
  } else if (key.case_compare("json_member_of") == 0) {
    if (OB_FAIL(resolve_json_member_of(*sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve json member_of", K(ret));
    }
  } else if (key.case_compare("json_overlaps") == 0) {
    if (OB_FAIL(resolve_json_overlaps(*sub_node, query, parent_query, outer_query_type))) {
      LOG_WARN("fail to resolve json overlaps", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid key in query");
    LOG_WARN("not supported syntax in query", K(ret), K(key));
  }
  if (OB_FAIL(ret)) {
  } else if (outer_query_type == QUERY_ITEM_QUERY && OB_FAIL(setup_top_level_score(query))) {
    LOG_WARN("fail to setup top level score", K(ret));
  } else if (outer_query_type == QUERY_ITEM_QUERY && OB_FAIL(try_push_nested_boost_to_leaf_query(query, 1.0))) {
    LOG_WARN("fail to push nested boost to leaf query", K(ret));
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
    dsl_query_info_->size_ = size_expr;
  }
  return ret;
}

int ObDSLResolver::resolve_slop(ObIJsonBase &req_node, int32_t &slop)
{
  int ret = OB_SUCCESS;
  double slop_double = 0.0;
  if (OB_FAIL(req_node.to_double(slop_double))) {
    LOG_WARN("fail to get double value from slop", K(ret));
  } else if (slop_double < 0.0 || slop_double != static_cast<int64_t>(slop_double)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("slop must be a non-negative integer", K(ret), K(slop_double));
  } else if (slop_double > INT32_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("slop exceeds int32_t max value", K(ret), K(slop_double));
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

int ObDSLResolver::resolve_minimum_should_match(ObIJsonBase &req_node, int32_t &msm)
{
  int ret = OB_SUCCESS;
  double msm_double = 0.0;
  if (req_node.json_type() != ObJsonNodeType::J_INT && req_node.json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "minimum_should_match, should be int or string type");
    LOG_WARN("minimum_should_match should be int type", K(ret), K(req_node.json_type()));
  } else if (OB_FAIL(req_node.to_double(msm_double))) {
    if (req_node.json_type() == ObJsonNodeType::J_STRING) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "minimum_should_match, it accepts an integer or a string representing an integer");
    }
    LOG_WARN("fail to get double value from minimum_should_match", K(ret));
  } else if (msm_double < 0.0 || msm_double > INT32_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "minimum_should_match, should be in range [0, 2147483647]");
    LOG_WARN("minimum_should_match out of range", K(ret), K(msm_double));
  } else {
    msm = static_cast<int32_t>(msm_double);
  }
  return ret;
}

int ObDSLResolver::resolve_multi_knn(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  ObJsonNodeType json_type = req_node.json_type();
  if (json_type != ObJsonNodeType::J_OBJECT && json_type != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("multi_knn must be object or array", K(ret), K(json_type));
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
  if (OB_ISNULL(expr_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr for expr factory", K(ret));
  } else if (OB_FAIL(ObDSLMultiMatchQuery::create(outer_query_type, parent_query, *allocator_, multi_match_query))) {
    LOG_WARN("fail to create multi match query", K(ret));
  } else if (OB_FAIL(resolved_field_idx_set.create(req_node.element_count()))) {
    LOG_WARN("fail to create resolved field idx set", K(ret));
  } else if (OB_UNLIKELY(req_node.json_type() != ObJsonNodeType::J_OBJECT)) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
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
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "boost in multi_match query");
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
  int32_t minimum_should_match = ObDSLFullTextQuery::DEFAULT_MINIMUM_SHOULD_MATCH;
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
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT,
              is_multi_match ? "type in multi_match query" : "type in query_string query");
        }
        LOG_WARN("fail to resolve query_string type", K(ret));
      }
    } else if (is_multi_match && key.case_compare("operator") == 0) {
      if (OB_FAIL(resolve_query_string_operator(*sub_node, opr))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "operator in multi_match query");
        LOG_WARN("fail to resolve query_string operator", K(ret));
      }
    } else if (!is_multi_match && key.case_compare("default_operator") == 0) {
      if (OB_FAIL(resolve_query_string_operator(*sub_node, opr))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "default_operator in query_string query");
        LOG_WARN("fail to resolve query_string operator", K(ret));
      }
    } else if (key.case_compare("minimum_should_match") == 0) {
      if (OB_FAIL(resolve_minimum_should_match(*sub_node, minimum_should_match))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT,
            is_multi_match ? "minimum_should_match in multi_match query"
                           : "minimum_should_match in query_string query");
        LOG_WARN("fail to resolve minimum_should_match", K(ret));
      }
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
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
      *params_.expr_factory_, ObIntType, minimum_should_match, minimum_should_match_expr))) {
    LOG_WARN("fail to build const int expr for minimum should match", K(ret));
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
  if (OB_FAIL(resolve_single_term(req_node, fts_query, nullptr, QUERY_ITEM_QUERY))) {
    LOG_WARN("failed to resolve single term", K(ret));
  } else if (OB_FAIL(dsl_query_info_->queries_.push_back(fts_query))) {
    LOG_WARN("fail to push back fts query", K(ret));
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
  if (OB_ISNULL(expr_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr for expr factory", K(ret));
  } else if (OB_FAIL(ObDSLQueryStringQuery::create(
      outer_query_type, parent_query, *allocator_, query_string_query))) {
    LOG_WARN("fail to create query string query", K(ret));
  } else if (OB_FAIL(resolved_field_idx_set.create(req_node.element_count()))) {
    LOG_WARN("fail to create resolved field idx set", K(ret));
  } else if (OB_UNLIKELY(req_node.json_type() != ObJsonNodeType::J_OBJECT)) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
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
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "boost in query_string query");
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
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
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
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("operator field should be string", K(ret), K(req_node.json_type()));
  } else if (OB_FALSE_IT(opr_str = ObString(req_node.get_data_length(), req_node.get_data()))) {
  } else if (opr_str.case_compare("or") == 0) {
    opr = MATCH_OPERATOR_OR;
  } else if (opr_str.case_compare("and") == 0) {
    opr = MATCH_OPERATOR_AND;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unsupported query_string operator", K(ret), K(opr_str));
  }
  return ret;
}

int ObDSLResolver::resolve_query_string_query(ObIJsonBase &req_node, ObConstRawExpr *&query_expr, ObCollationType collation_type)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = nullptr;
  if (req_node.json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("query should be string", K(ret), K(req_node.json_type()));
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
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("type field should be string", K(ret), K(req_node.json_type()));
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
  ObRawExpr *path_expr = nullptr;
  ObSysFunRawExpr *json_extract_expr = nullptr;
  ObRawExpr *field_expr = nullptr;
  ObRawExpr *scalar_expr = nullptr;
  ObSEArray<ObRawExpr*, 4, ModulePageAllocator, true> condition_exprs;
  ObDSLScalarQuery *range_query = nullptr;
  if (ObDSLQuery::check_need_cal_score_in_bool(outer_query_type, parent_query)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "scalar range query in must/should clause");
    LOG_WARN("scalar range query cannot be scored or exist in must/should clause", K(ret), K(outer_query_type));
  } else if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else if (req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("range must only contain one term", K(ret));
  } else if (OB_FAIL(req_node.get_object_value(0, col_name, sub_node))) {
    LOG_WARN("fail to get object key and value", K(ret));
  } else if (OB_FALSE_IT(count = sub_node->element_count())) {
  } else if (count == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpectd range condition", K(ret));
  } else if (OB_FAIL(get_field_expr_and_path(col_name, col_expr, path_str))) {
    LOG_WARN("fail to get field expr and path", K(ret), K(col_name));
  } else if (path_str.empty()) {
    if (OB_FAIL(dsl_query_info_->dsl_cols.push_back(col_expr))) {
      LOG_WARN("failed to push back scalar query column expr", K(ret));
    } else {
      field_expr = col_expr;
    }
  } else if (OB_FAIL(construct_string_expr(path_str, path_expr))) {
    LOG_WARN("fail to create path expr", K(ret), K(path_str));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SYS_JSON_EXTRACT, json_extract_expr))) {
    LOG_WARN("fail to create json_extract expr", K(ret));
  } else if (OB_FALSE_IT(json_extract_expr->set_func_name(N_JSON_EXTRACT))) {
  } else if (OB_FAIL(json_extract_expr->set_param_exprs(col_expr, path_expr))) {
    LOG_WARN("fail to set param exprs for json_extract", K(ret));
  } else {
    field_expr = json_extract_expr;
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
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid key in range query");
      LOG_WARN("not supported syntax in query", K(ret), K(key));
    }
    if (OB_FAIL(ret)) {
    } else if (!IS_RANGE_CMP_OP(type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid type", K(ret), K(type));
    } else if (!is_scalar_json_type(var_node->json_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "value in range query, should be string or number");
      LOG_WARN("invalid value type in range query", K(ret), K(var_node->json_type()));
    } else if (col_expr->get_result_type().get_type() == ObTinyIntType &&
               col_expr->get_result_type().get_precision() == DEFAULT_PRECISION_FOR_BOOL) {
      value_type = ObJsonNodeType::J_BOOLEAN;
    } else {
      value_type = var_node->json_type();
    }
    if (OB_FAIL(ret)) {
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
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty condition exprs", K(ret));
  } else if (OB_FAIL(ObDSLScalarQuery::create(*allocator_, range_query, QUERY_ITEM_RANGE, outer_query_type, parent_query))) {
    LOG_WARN("fail to create range query", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_and_expr(*expr_factory, condition_exprs, scalar_expr))) {
    LOG_WARN("fail to build and expr", K(ret));
  } else {
    range_query->field_ = field_expr;
    range_query->scalar_expr_ = scalar_expr;
    range_query->boost_ = setup_boost(nullptr);
    query = range_query;
  }
  return ret;
}

int ObDSLResolver::resolve_rank(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  ObString key;
  ObIJsonBase *sub_node = nullptr;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpected json type", K(ret), K(req_node.json_type()));
  } else if (req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rank must only contain one method", K(ret));
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
  return ret;
}

int ObDSLResolver::resolve_rrf(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpected json type", K(ret), K(req_node.json_type()));
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
  ObString col_name;
  ObString path_str;
  ObJsonNodeType value_type = ObJsonNodeType::J_MAX_TYPE;
  ObIJsonBase *col_para = nullptr;
  ObIJsonBase *value_node = nullptr;
  ObColumnRefRawExpr *col_expr = nullptr;
  ObRawExpr *path_expr = nullptr;
  ObSysFunRawExpr *json_extract_expr = nullptr;
  ObRawExpr *field_expr = nullptr;
  ObRawExpr *value_expr = nullptr;
  ObRawExpr *eq_expr = nullptr;
  ObDSLScalarQuery *term_query = nullptr;
  if (ObDSLQuery::check_need_cal_score_in_bool(outer_query_type, parent_query)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "scalar term query in must/should clause");
    LOG_WARN("scalar term query cannot be scored or exist in must/should clause", K(ret), K(outer_query_type));
  } else if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("term expr should be object", K(ret));
  } else if (req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("term expr should have exactly one element", K(ret));
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
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid key in term expr");
        LOG_WARN("unsupported key in term expr", K(ret), K(key));
      }
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(col_para->json_type()));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(value_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null value node", K(ret));
  } else if (!col_expr->get_result_type().is_json() &&
             !col_expr->get_result_type().is_collection_sql_type() &&
             !is_scalar_json_type(value_node->json_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "value in term query, should be string or number for non-json and non-array column");
    LOG_WARN("invalid value type in term query", K(ret), K(col_expr->get_result_type()), K(value_node->json_type()));
  } else if (col_expr->get_result_type().get_type() == ObTinyIntType &&
             col_expr->get_result_type().get_precision() == DEFAULT_PRECISION_FOR_BOOL) {
    value_type = ObJsonNodeType::J_BOOLEAN;
  } else {
    value_type = value_node->json_type();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(resolve_const(*value_node, value_expr, value_type))) {
    LOG_WARN("fail to resolve const value", K(ret));
  } else if (path_str.empty()) {
    if (OB_FAIL(dsl_query_info_->dsl_cols.push_back(col_expr))) {
      LOG_WARN("failed to push back scalar query column expr", K(ret));
    } else {
      field_expr = col_expr;
    }
  } else if (OB_FAIL(construct_string_expr(path_str, path_expr))) {
    LOG_WARN("fail to create path expr", K(ret), K(path_str));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SYS_JSON_EXTRACT, json_extract_expr))) {
    LOG_WARN("fail to create json_extract expr", K(ret));
  } else if (OB_FALSE_IT(json_extract_expr->set_func_name(N_JSON_EXTRACT))) {
  } else if (OB_FAIL(json_extract_expr->set_param_exprs(col_expr, path_expr))) {
    LOG_WARN("fail to set param exprs for json_extract", K(ret));
  } else {
    field_expr = json_extract_expr;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(*expr_factory, T_OP_EQ, field_expr, value_expr, eq_expr))) {
    LOG_WARN("fail to build equal expr", K(ret));
  } else if (OB_FAIL(ObDSLScalarQuery::create(*allocator_, term_query, QUERY_ITEM_TERM, outer_query_type, parent_query))) {
    LOG_WARN("fail to create term query", K(ret));
  } else {
    term_query->field_ = field_expr;
    term_query->scalar_expr_ = eq_expr;
    term_query->boost_ = setup_boost(nullptr);
    query = term_query;
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
  ObColumnRefRawExpr *col_expr = nullptr;
  ObRawExpr *path_expr = nullptr;
  ObSysFunRawExpr *json_extract_expr = nullptr;
  ObRawExpr *field_expr = nullptr;
  ObSEArray<ObRawExpr*, 4, ModulePageAllocator, true> value_exprs;
  ObRawExpr *in_expr = nullptr;
  ObDSLScalarQuery *terms_query = nullptr;
  if (ObDSLQuery::check_need_cal_score_in_bool(outer_query_type, parent_query)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "scalar terms query in must/should clause");
    LOG_WARN("scalar terms query cannot be scored or exist in must/should clause", K(ret), K(outer_query_type));
  } else if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("terms expr should be object", K(ret));
  } else if (FALSE_IT(count = req_node.element_count())) {
  } else if (count != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("terms expr only supports field and boost", K(ret));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    ObString key;
    ObIJsonBase *sub_node = nullptr;
    if (OB_FAIL(req_node.get_object_value(i, key, sub_node))) {
      LOG_WARN("fail to get value", K(ret));
    } else if (OB_NOT_NULL(col_expr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("terms expr only supports one field", K(ret));
    } else if (sub_node->json_type() != ObJsonNodeType::J_ARRAY) {
      ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
      LOG_WARN("unexpected value type, should be array", K(ret), K(sub_node->json_type()));
    } else if (OB_FAIL(get_field_expr_and_path(key, col_expr, path_str))) {
      LOG_WARN("fail to get field expr and path", K(ret), K(key));
    } else {
      uint64_t array_count = sub_node->element_count();
      if (array_count == 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("keyword array should have at least one element", K(ret));
      } else if (col_expr->get_result_type().is_json()) {
        query_type = QUERY_ITEM_JSON_OVERLAPS;
      } else if (col_expr->get_result_type().is_collection_sql_type()) {
        query_type = QUERY_ITEM_ARRAY_CONTAINS;
      }
      for (uint64_t j = 0; OB_SUCC(ret) && j < array_count; j++) {
        ObJsonNodeType element_type = ObJsonNodeType::J_MAX_TYPE;
        ObIJsonBase *element = nullptr;
        ObRawExpr *value_expr = nullptr;
        if (OB_FAIL(sub_node->get_array_element(j, element))) {
          LOG_WARN("fail to get array element", K(ret), K(j));
        } else if (!col_expr->get_result_type().is_json() &&
                   !col_expr->get_result_type().is_collection_sql_type() &&
                   !is_scalar_json_type(element->json_type())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "value in terms query, should be scalar for non-json and non-array column");
          LOG_WARN("invalid value type in terms query", K(ret), K(col_expr->get_result_type()), K(element->json_type()));
        } else if (col_expr->get_result_type().get_type() == ObTinyIntType &&
                   col_expr->get_result_type().get_precision() == DEFAULT_PRECISION_FOR_BOOL) {
          element_type = ObJsonNodeType::J_BOOLEAN;
        } else {
          element_type = element->json_type();
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(resolve_const(*element, value_expr, element_type, query_type))) {
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
    LOG_WARN("terms expr should have field", K(ret));
  } else if (path_str.empty()) {
    if (OB_FAIL(dsl_query_info_->dsl_cols.push_back(col_expr))) {
      LOG_WARN("failed to push back scalar query column expr", K(ret));
    } else {
      field_expr = col_expr;
    }
  } else if (OB_FAIL(construct_string_expr(path_str, path_expr))) {
    LOG_WARN("fail to create path expr", K(ret), K(path_str));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_SYS_JSON_EXTRACT, json_extract_expr))) {
    LOG_WARN("fail to create json_extract expr", K(ret));
  } else if (OB_FALSE_IT(json_extract_expr->set_func_name(N_JSON_EXTRACT))) {
  } else if (OB_FAIL(json_extract_expr->set_param_exprs(col_expr, path_expr))) {
    LOG_WARN("fail to set param exprs for json_extract", K(ret));
  } else {
    field_expr = json_extract_expr;
  }
  if (OB_FAIL(ret)) {
  } else if (value_exprs.count() == 1) {
    query_type = QUERY_ITEM_TERM;
    if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(*expr_factory, T_OP_EQ, field_expr, value_exprs.at(0), in_expr))) {
      LOG_WARN("fail to build equal expr", K(ret));
    }
  } else if (col_expr->get_result_type().is_collection_sql_type()) {
    ObSysFunRawExpr *array_expr = nullptr;
    ObSysFunRawExpr *array_contains_expr = nullptr;
    // for an array column, use array_contains to replace in operator
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
    } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUNC_SYS_ARRAY_CONTAINS, array_contains_expr))) {
      LOG_WARN("fail to create array contains expr", K(ret));
    } else if (OB_FALSE_IT(array_contains_expr->set_func_name(N_ARRAY_CONTAINS))) {
    } else if (OB_FAIL(array_contains_expr->set_param_exprs(array_expr, col_expr))) {
      LOG_WARN("fail to set param exprs for array contains", K(ret));
    } else {
      in_expr = array_contains_expr;
    }
  } else if (col_expr->get_result_type().is_json()) {
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
    } else if (OB_FAIL(json_overlaps_expr->set_param_exprs(field_expr, json_array_expr))) {
      LOG_WARN("fail to set param exprs for json overlaps", K(ret));
    } else {
      in_expr = json_overlaps_expr;
    }
  } else {
    ObOpRawExpr *row_expr = nullptr;
    if (OB_FAIL(expr_factory->create_raw_expr(T_OP_ROW, row_expr))) {
      LOG_WARN("fail to create row expr", K(ret));
    } else if (OB_FAIL(row_expr->set_param_exprs(value_exprs))) {
      LOG_WARN("fail to set param exprs for row expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(*expr_factory, T_OP_IN, field_expr, row_expr, in_expr))) {
      LOG_WARN("fail to build in expr", K(ret));
    } else {
      static_cast<ObOpRawExpr*>(in_expr)->set_add_implicit_cast_for_in_param(true);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDSLScalarQuery::create(*allocator_, terms_query, query_type, outer_query_type, parent_query))) {
    LOG_WARN("fail to create terms query", K(ret));
  } else {
    terms_query->field_ = field_expr;
    terms_query->scalar_expr_ = in_expr;
    terms_query->boost_ = setup_boost(nullptr);
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
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpected json type", K(ret), K(req_node.json_type()));
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
        ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
        LOG_WARN("normalizer must be string", K(ret), K(sub_node->json_type()));
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
    if (OB_ISNULL(bool_query)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bool query is null", K(ret));
    } else {
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
  }
  return ret;
}

int ObDSLResolver::set_default_rank_window_size()
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = params_.expr_factory_;
  if (OB_ISNULL(dsl_query_info_->from_) ||
      OB_ISNULL(dsl_query_info_->size_) ||
      OB_NOT_NULL(dsl_query_info_->rank_info_.window_size_) ||
      OB_ISNULL(expr_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null from or size expr or expr factory", K(ret));
  } else {
    ObConstRawExpr *size_const = static_cast<ObConstRawExpr *>(dsl_query_info_->size_);
    ObConstRawExpr *from_const = static_cast<ObConstRawExpr *>(dsl_query_info_->from_);
    int64_t size_val = 0;
    int64_t from_val = 0;
    ObConstRawExpr *sum_expr = nullptr;
    if (OB_FAIL(size_const->get_value().get_int(size_val))) {
      LOG_WARN("failed to get size const value", K(ret));
    } else if (OB_FAIL(from_const->get_value().get_int(from_val))) {
      LOG_WARN("failed to get from const value", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, size_val + from_val, sum_expr))) {
      LOG_WARN("failed to build size+from const expr", K(ret));
    } else {
      dsl_query_info_->rank_info_.window_size_ = sum_expr;
    }
  }
  return ret;
}

int ObDSLResolver::trim_strtod(const ObString &num_str, double &num_val)
{
  int ret = OB_SUCCESS;
  int64_t str_len = 0;
  char *buf = nullptr;
  ObString trimmed_str = num_str.trim();
  str_len = trimmed_str.length();
  if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(str_len + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for temp string", K(ret));
  } else {
    MEMCPY(buf, trimmed_str.ptr(), str_len);
    buf[str_len] = '\0';  // need a terminated string for strtod
    char *end_ptr = nullptr;
    num_val = strtod(buf, &end_ptr);
    if (end_ptr == buf || end_ptr != buf + str_len) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid numeric string", K(ret), K(num_str));
    }
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
  } else if (IS_QUERY_ITEM_FULLTEXT(query->query_type_)) {
    // update leaf query boost
    ObDSLFullTextQuery *fulltext_query = static_cast<ObDSLFullTextQuery*>(query);
    if (OB_ISNULL(fulltext_query->boost_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null boost expr", K(ret));
    } else {
      const double new_boost = cumulative_boost * fulltext_query->boost_->get_value().get_double();
      // if boost_ is the shared one_const_expr_, we need to create a new expression
      // instead of modifying the shared one, to avoid affecting other queries that use it
      if (fulltext_query->boost_ == dsl_query_info_->one_const_expr_) {
        ObConstRawExpr *new_boost_expr = nullptr;
        ObRawExprFactory *expr_factory = params_.expr_factory_;
        if (OB_FAIL(ObRawExprUtils::build_const_double_expr(*expr_factory, ObDoubleType, new_boost, new_boost_expr))) {
          LOG_WARN("fail to create new boost expr", K(ret));
        } else {
          fulltext_query->boost_ = new_boost_expr;
        }
      } else {
        ObObj new_boost_obj;
        new_boost_obj.set_double(new_boost);
        fulltext_query->boost_->set_value(new_boost_obj);
      }
    }
  } else if (IS_QUERY_ITEM_BOOL(query->query_type_)) {
    // accumulate & propagate boost
    ObDSLBoolQuery *bool_query = static_cast<ObDSLBoolQuery*>(query);
    double new_boost = 0;
    if (OB_ISNULL(bool_query->boost_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null boost expr", K(ret));
    } else {
      new_boost = cumulative_boost * bool_query->boost_->get_value().get_double();
    }
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

}  // namespace sql
}  // namespace oceanbase
