/**
 * Copyright (c) 2025 OceanBase
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
#include "sql/hybrid_search/ob_fulltext_search_query.h"
#include "sql/hybrid_search/ob_fulltext_search_node.h"
#include "sql/rewrite/ob_transform_utils.h"

namespace oceanbase
{
namespace sql
{

ObDSLFullTextMultiFieldQueryParam::ObDSLFullTextMultiFieldQueryParam(ObIAllocator &alloc)
  : fields_(&alloc),
    field_boosts_(&alloc),
    minimum_should_match_(nullptr),
    operator_(nullptr),
    field_type_(nullptr)
{}

bool ObDSLFullTextMultiFieldQueryParam::is_valid() const
{
  return nullptr != minimum_should_match_
      && nullptr != operator_
      && nullptr != field_type_
      && fields_.count() == field_boosts_.count()
      && fields_.count() > 0;
}

int ObDSLFullTextMultiFieldQueryParam::deep_copy(
    ObIRawExprCopier &expr_copier,
    ObDSLFullTextMultiFieldQueryParam &dst) const
{
  int ret = OB_SUCCESS;
  ObRawExpr *minimum_should_match_expr = nullptr;
  ObRawExpr *operator_expr = nullptr;
  ObRawExpr *field_type_expr = nullptr;
  const int64_t field_count = fields_.count();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid multi field query param", K(ret));
  } else if (OB_FAIL(expr_copier.copy(minimum_should_match_, minimum_should_match_expr))) {
    LOG_WARN("fail to copy minimum should match expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(operator_, operator_expr))) {
    LOG_WARN("fail to copy operator expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(field_type_, field_type_expr))) {
    LOG_WARN("fail to copy field type expr", K(ret));
  } else if (OB_FAIL(dst.fields_.reserve(field_count))) {
    LOG_WARN("fail to reserve fields", K(ret));
  } else if (OB_FAIL(dst.field_boosts_.reserve(field_count))) {
    LOG_WARN("fail to reserve field boosts", K(ret));
  } else {
    dst.minimum_should_match_ = static_cast<ObConstRawExpr*>(minimum_should_match_expr);
    dst.operator_ = static_cast<ObConstRawExpr*>(operator_expr);
    dst.field_type_ = static_cast<ObConstRawExpr*>(field_type_expr);
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < field_count; ++i) {
    ObRawExpr *field_expr = nullptr;
    ObRawExpr *boost_expr = nullptr;
    if (OB_FAIL(expr_copier.copy(fields_.at(i), field_expr))) {
      LOG_WARN("fail to copy field expr", K(ret));
    } else if (OB_FAIL(expr_copier.copy(field_boosts_.at(i), boost_expr))) {
      LOG_WARN("fail to copy field boost expr", K(ret));
    } else if (OB_FAIL(dst.fields_.push_back(static_cast<ObColumnRefRawExpr*>(field_expr)))) {
      LOG_WARN("fail to push back field expr", K(ret));
    } else if (OB_FAIL(dst.field_boosts_.push_back(static_cast<ObConstRawExpr*>(boost_expr)))) {
      LOG_WARN("fail to push back field boost expr", K(ret));
    }
  }
  return ret;
}

int ObDSLFullTextMultiFieldQueryParam::collect_exprs(ObIArray<ObRawExpr *> &exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(exprs.push_back(minimum_should_match_))) {
    LOG_WARN("fail to push back minimum should match expr", K(ret));
  } else if (OB_FAIL(exprs.push_back(operator_))) {
    LOG_WARN("fail to push back operator expr", K(ret));
  } else if (OB_FAIL(exprs.push_back(field_type_))) {
    LOG_WARN("fail to push back field type expr", K(ret));
  } else if (OB_FAIL(append(exprs, fields_))) {
    LOG_WARN("fail to append fields", K(ret));
  } else if (OB_FAIL(append(exprs, field_boosts_))) {
    LOG_WARN("fail to append field boosts", K(ret));
  }
  return ret;
}

int ObDSLFullTextQuery::deep_copy(
    ObDSLQuery *parent,
    ObIRawExprCopier &expr_copier,
    ObIAllocator &allocator,
    ObDSLFullTextQuery *&dst) const
{
  return OB_NOT_IMPLEMENT;
}

int ObDSLFullTextQuery::inner_deep_copy(ObIRawExprCopier &expr_copier, ObDSLFullTextQuery &dst) const
{
  int ret = OB_SUCCESS;
  ObRawExpr *query_expr = nullptr;
  ObRawExpr *boost_expr = nullptr;
  if (OB_FAIL(expr_copier.copy(query_, query_expr))) {
    LOG_WARN("fail to copy query expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(boost_, boost_expr))) {
    LOG_WARN("fail to copy boost expr", K(ret));
  } else {
    dst.query_ = static_cast<ObConstRawExpr*>(query_expr);
    dst.boost_ = static_cast<ObConstRawExpr*>(boost_expr);
  }
  return ret;
}

int ObDSLFullTextQuery::collect_exprs(ObIArray<ObRawExpr *> &exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(exprs.push_back(query_))) {
    LOG_WARN("fail to push back query expr", K(ret));
  } else if (OB_FAIL(exprs.push_back(boost_))) {
    LOG_WARN("fail to push back boost expr", K(ret));
  }
  return ret;
}

int ObDSLFullTextQuery::produce_query_node(
    const ObTableSchema &main_table_schema,
    const ObSqlSchemaGuard &schema_guard,
    ObIAllocator &alloc,
    ObFullTextQueryNode *&query_node) const
{
  return OB_NOT_IMPLEMENT;
}

int ObDSLFullTextQuery::resolve_index_with_field(
  const ObColumnRefRawExpr &field,
  const ObTableSchema &main_table_schema,
  const ObSqlSchemaGuard &schema_guard,
  uint64_t &inv_idx_tid,
  uint64_t &doc_id_idx_tid,
  ObString &index_name)
{
  int ret = OB_SUCCESS;
  doc_id_idx_tid = OB_INVALID_ID;
  inv_idx_tid = OB_INVALID_ID;
  uint64_t doc_id_col_id = OB_INVALID_ID;
  ObSEArray<ObAuxTableMetaInfo, 4> index_infos;
  ColumnReferenceSet column_set;

  if (OB_FAIL(column_set.add_member(field.get_column_id()))) {
    LOG_WARN("failed to add column to column set", K(ret));
  } else if (OB_FAIL(main_table_schema.get_simple_index_infos(index_infos))) {
    LOG_WARN("failed to get index infos", K(ret));
  } else if (OB_FAIL(main_table_schema.get_docid_col_id(doc_id_col_id))) {
    if (OB_ERR_INDEX_KEY_NOT_FOUND == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to check docid in schema", K(ret));
    }
  } else if (OB_INVALID_ID != doc_id_col_id) {
    // check doc_rowkey index table valid
    for (int64_t i = 0; i < index_infos.count(); ++i) {
      if (share::schema::is_doc_rowkey_aux(index_infos.at(i).index_type_)) {
        doc_id_idx_tid = index_infos.at(i).table_id_;
        break;
      }
    }
    const ObTableSchema *doc_id_idx_schema = nullptr;
    if (OB_UNLIKELY(OB_INVALID_ID == doc_id_idx_tid)) {
      ret = OB_ERR_FT_COLUMN_NOT_INDEXED;
      LOG_WARN("doc rowkey index not found for table with doc id column", K(ret), K(main_table_schema));
    } else if (OB_FAIL(schema_guard.get_table_schema(doc_id_idx_tid, doc_id_idx_schema))) {
      LOG_WARN("failed to get doc_id_idx_table_schema", K(ret));
    } else if (OB_ISNULL(doc_id_idx_schema)) {
      ret = OB_ERR_FT_COLUMN_NOT_INDEXED;
      LOG_WARN("unexpected doc_id_idx_table_schema", K(ret));
    } else if (!doc_id_idx_schema->can_read_index() || !doc_id_idx_schema->is_index_visible()) {
      ret = OB_ERR_FT_COLUMN_NOT_INDEXED;
      LOG_WARN("doc_id_idx_table_schema is not visible", K(ret), KPC(doc_id_idx_schema));
    }
  }

  const ObTableSchema *inv_idx_schema = nullptr;
  bool found_matched_index = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count() && !found_matched_index; ++i) {
    const ObAuxTableMetaInfo &index_info = index_infos.at(i);
    if (!share::schema::is_fts_index_aux(index_info.index_type_)) {
      // skip
    } else if (OB_FAIL(schema_guard.get_table_schema(index_info.table_id_, inv_idx_schema))) {
      LOG_WARN("failed to get index schema", K(ret));
    } else if (OB_ISNULL(inv_idx_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected index schema", K(ret), KPC(inv_idx_schema));
    } else if (!inv_idx_schema->can_read_index() || !inv_idx_schema->is_index_visible()) {
      // index unavailable
    } else if (OB_FAIL(ObTransformUtils::check_fulltext_index_match_column(
        column_set, &main_table_schema, inv_idx_schema, found_matched_index))) {
      LOG_WARN("failed to check fulltext index match column", K(ret));
    } else if (found_matched_index) {
      inv_idx_tid = index_info.table_id_;
      if (OB_FAIL(inv_idx_schema->get_index_name(index_name))) {
        LOG_WARN("fail to get index name", KR(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!found_matched_index)) {
    ret = OB_ERR_FT_COLUMN_NOT_INDEXED;
    LOG_WARN("no matched fulltext index found", K(ret), K(index_infos));
  }

  return ret;
}

int ObDSLMatchQuery::create(
    ObEsQueryItem outer_query_type,
    ObDSLQuery *parent_query,
    ObIAllocator &alloc,
    ObDSLMatchQuery *&match_query)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(match_query = OB_NEWx(ObDSLMatchQuery, &alloc, outer_query_type, parent_query))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObDSLMatchQuery", K(ret));
  }
  return ret;
}

int ObDSLMatchQuery::deep_copy(
    ObDSLQuery *parent,
    ObIRawExprCopier &expr_copier,
    ObIAllocator &allocator,
    ObDSLFullTextQuery *&dst) const
{
  int ret = OB_SUCCESS;
  ObDSLMatchQuery *tmp_dst = nullptr;
  if (OB_FAIL(ObDSLMatchQuery::create(outer_query_type_, parent, allocator, tmp_dst))) {
    LOG_WARN("fail to create match query", K(ret));
  } else if (OB_FAIL(inner_deep_copy(expr_copier, *tmp_dst))) {
    LOG_WARN("fail to inner deep copy match query", K(ret));
  } else {
    dst = tmp_dst;
  }
  return ret;
}

int ObDSLMatchQuery::inner_deep_copy(ObIRawExprCopier &expr_copier, ObDSLMatchQuery &dst) const
{
  int ret = OB_SUCCESS;
  ObRawExpr *field_expr = nullptr;
  ObRawExpr *minimum_should_match_expr = nullptr;
  ObRawExpr *operator_expr = nullptr;
  if (OB_FAIL(ObDSLFullTextQuery::inner_deep_copy(expr_copier, dst))) {
    LOG_WARN("fail to inner deep copy fulltext query", K(ret));
  } else if (OB_FAIL(expr_copier.copy(field_, field_expr))) {
    LOG_WARN("fail to copy field expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(minimum_should_match_, minimum_should_match_expr))) {
    LOG_WARN("fail to copy minimum should match expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(operator_, operator_expr))) {
    LOG_WARN("fail to copy operator expr", K(ret));
  } else {
    dst.field_ = static_cast<ObColumnRefRawExpr*>(field_expr);
    dst.minimum_should_match_ = static_cast<ObConstRawExpr*>(minimum_should_match_expr);
    dst.operator_ = static_cast<ObConstRawExpr*>(operator_expr);
  }
  return ret;
}

int ObDSLMatchQuery::collect_exprs(ObIArray<ObRawExpr *> &exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDSLFullTextQuery::collect_exprs(exprs))) {
    LOG_WARN("fail to collect fulltext query exprs", K(ret));
  } else if (OB_FAIL(exprs.push_back(field_))) {
    LOG_WARN("fail to push back field expr", K(ret));
  } else if (OB_FAIL(exprs.push_back(minimum_should_match_))) {
    LOG_WARN("fail to push back minimum should match expr", K(ret));
  } else if (OB_FAIL(exprs.push_back(operator_))) {
    LOG_WARN("fail to push back operator expr", K(ret));
  }
  return ret;
}

int ObDSLMatchQuery::produce_query_node(
    const ObTableSchema &main_table_schema,
    const ObSqlSchemaGuard &schema_guard,
    ObIAllocator &alloc,
    ObFullTextQueryNode *&query_node) const
{
  int ret = OB_SUCCESS;
  ObMatchQueryNode *match_query_node = nullptr;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid match query", K(ret), KPC(this));
  } else if (OB_ISNULL(match_query_node = OB_NEWx(ObMatchQueryNode, &alloc, alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObMatchQueryNode", K(ret));
  } else if (OB_FAIL(ObDSLFullTextQuery::resolve_index_with_field(
      *field_,
      main_table_schema,
      schema_guard,
      match_query_node->inv_idx_tid_,
      match_query_node->doc_id_idx_tid_,
      match_query_node->index_name_))) {
    LOG_WARN("failed to resolve fulltext index with field", K(ret));
  } else  {
    match_query_node->query_text_ = query_;
    match_query_node->boost_ = boost_;
    match_query_node->field_ = field_;
    match_query_node->minimum_should_match_ = minimum_should_match_;
    match_query_node->operator_ = operator_;
  }

  if (OB_FAIL(ret)) {
    match_query_node->~ObMatchQueryNode();
    alloc.free(match_query_node);
    match_query_node = nullptr;
  } else {
    query_node = match_query_node;
  }
  return ret;
}

bool ObDSLMatchQuery::is_valid() const
{
  return ObDSLFullTextQuery::is_valid()
      && nullptr != field_
      && nullptr != minimum_should_match_
      && nullptr != operator_;
}

int ObDSLMultiMatchQuery::create(
    ObEsQueryItem outer_query_type,
    ObDSLQuery *parent_query,
    ObIAllocator &alloc,
    ObDSLMultiMatchQuery *&multi_match_query)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(multi_match_query = OB_NEWx(ObDSLMultiMatchQuery, &alloc, outer_query_type, parent_query, alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObDSLMultiMatchQuery", K(ret));
  }
  return ret;
}

int ObDSLMultiMatchQuery::deep_copy(
    ObDSLQuery *parent,
    ObIRawExprCopier &expr_copier,
    ObIAllocator &allocator,
    ObDSLFullTextQuery *&dst) const
{
  int ret = OB_SUCCESS;
  ObDSLMultiMatchQuery *tmp_dst = nullptr;
  if (OB_FAIL(ObDSLMultiMatchQuery::create(outer_query_type_, parent, allocator, tmp_dst))) {
    LOG_WARN("fail to create multi match query", K(ret));
  } else if (OB_FAIL(inner_deep_copy(expr_copier, *tmp_dst))) {
    LOG_WARN("fail to inner deep copy multi match query", K(ret));
  } else {
    dst = tmp_dst;
  }
  return ret;
}

int ObDSLMultiMatchQuery::inner_deep_copy(ObIRawExprCopier &expr_copier, ObDSLMultiMatchQuery &dst) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDSLFullTextQuery::inner_deep_copy(expr_copier, dst))) {
    LOG_WARN("fail to inner deep copy fulltext query", K(ret));
  } else if (OB_FAIL(fields_param_.deep_copy(expr_copier, dst.fields_param_))) {
    LOG_WARN("fail to deep copy fields param", K(ret));
  }
  return ret;
}

int ObDSLMultiMatchQuery::collect_exprs(ObIArray<ObRawExpr *> &exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDSLFullTextQuery::collect_exprs(exprs))) {
    LOG_WARN("fail to collect fulltext query exprs", K(ret));
  } else if (OB_FAIL(fields_param_.collect_exprs(exprs))) {
    LOG_WARN("fail to collect fields param exprs", K(ret));
  }
  return ret;
}

int ObDSLMultiMatchQuery::produce_query_node(
    const ObTableSchema &main_table_schema,
    const ObSqlSchemaGuard &schema_guard,
    ObIAllocator &alloc,
    ObFullTextQueryNode *&query_node) const
{
  int ret = OB_SUCCESS;
  ObMultiMatchQueryNode *multi_match_query_node = nullptr;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid multi match query", K(ret), KPC(this));
  } else if (OB_ISNULL(multi_match_query_node = OB_NEWx(ObMultiMatchQueryNode, &alloc, alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObMultiMatchQueryNode", K(ret));
  } else {
    multi_match_query_node->query_text_ = query_;
    multi_match_query_node->boost_ = boost_;
    multi_match_query_node->minimum_should_match_ = fields_param_.minimum_should_match_;
    multi_match_query_node->operator_ = fields_param_.operator_;
    multi_match_query_node->field_type_ = fields_param_.field_type_;
    multi_match_query_node->fields_.set_allocator(&alloc);
    multi_match_query_node->field_boosts_.set_allocator(&alloc);
    multi_match_query_node->inv_idx_tids_.set_allocator(&alloc);
    multi_match_query_node->index_infos_.set_allocator(&alloc);
    multi_match_query_node->index_names_.set_allocator(&alloc);
    if (OB_FAIL(multi_match_query_node->fields_.assign(fields_param_.fields_))) {
      LOG_WARN("fail to assign fields", K(ret));
    } else if (OB_FAIL(multi_match_query_node->field_boosts_.assign(fields_param_.field_boosts_))) {
      LOG_WARN("fail to assign field boosts", K(ret));
    } else if (OB_FAIL(multi_match_query_node->inv_idx_tids_.prepare_allocate(fields_param_.field_count()))) {
      LOG_WARN("fail to prepare allocate inv idx tids", K(ret));
    } else if (OB_FAIL(multi_match_query_node->index_infos_.prepare_allocate(fields_param_.field_count()))) {
      LOG_WARN("fail to prepare allocate index infos", K(ret));
    } else if (OB_FAIL(multi_match_query_node->index_names_.prepare_allocate(fields_param_.field_count()))) {
      LOG_WARN("fail to prepare allocate index names", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < fields_param_.field_count(); ++i) {
      if (OB_FAIL(ObDSLFullTextQuery::resolve_index_with_field(
          *fields_param_.fields_.at(i),
          main_table_schema,
          schema_guard,
          multi_match_query_node->inv_idx_tids_.at(i),
          multi_match_query_node->doc_id_idx_tid_,
          multi_match_query_node->index_names_.at(i)))) {
        LOG_WARN("failed to resolve fulltext index with field", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      multi_match_query_node->~ObMultiMatchQueryNode();
      alloc.free(multi_match_query_node);
      multi_match_query_node = nullptr;
    } else {
      query_node = multi_match_query_node;
    }
  }
  return ret;
}

bool ObDSLMultiMatchQuery::is_valid() const
{
  return ObDSLFullTextQuery::is_valid() && fields_param_.is_valid();
}

int ObDSLMatchPhraseQuery::create(
    ObEsQueryItem outer_query_type,
    ObDSLQuery *parent_query,
    ObIAllocator &alloc,
    ObDSLMatchPhraseQuery *&match_phrase_query)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(match_phrase_query = OB_NEWx(ObDSLMatchPhraseQuery, &alloc, outer_query_type, parent_query))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObDSLMatchPhraseQuery", K(ret));
  }
  return ret;
}

int ObDSLMatchPhraseQuery::deep_copy(
    ObDSLQuery *parent,
    ObIRawExprCopier &expr_copier,
    ObIAllocator &allocator,
    ObDSLFullTextQuery *&dst) const
{
  int ret = OB_SUCCESS;
  ObDSLMatchPhraseQuery *tmp_dst = nullptr;
  if (OB_FAIL(ObDSLMatchPhraseQuery::create(outer_query_type_, parent, allocator, tmp_dst))) {
    LOG_WARN("fail to create match phrase query", K(ret));
  } else if (OB_FAIL(inner_deep_copy(expr_copier, *tmp_dst))) {
    LOG_WARN("fail to inner deep copy match phrase query", K(ret));
  } else {
    dst = tmp_dst;
  }
  return ret;
}

int ObDSLMatchPhraseQuery::inner_deep_copy(
    ObIRawExprCopier &expr_copier,
    ObDSLMatchPhraseQuery &dst) const
{
  int ret = OB_SUCCESS;
  ObRawExpr *field_expr = nullptr;
  ObRawExpr *slop_expr = nullptr;
  if (OB_FAIL(ObDSLFullTextQuery::inner_deep_copy(expr_copier, dst))) {
    LOG_WARN("fail to inner deep copy fulltext query", K(ret));
  } else if (OB_FAIL(expr_copier.copy(field_, field_expr))) {
    LOG_WARN("fail to copy field expr", K(ret));
  } else if (OB_FAIL(expr_copier.copy(slop_, slop_expr))) {
    LOG_WARN("fail to copy slop expr", K(ret));
  } else {
    dst.field_ = static_cast<ObColumnRefRawExpr*>(field_expr);
    dst.slop_ = static_cast<ObConstRawExpr*>(slop_expr);
  }
  return ret;
}

int ObDSLMatchPhraseQuery::collect_exprs(ObIArray<ObRawExpr *> &exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDSLFullTextQuery::collect_exprs(exprs))) {
    LOG_WARN("fail to collect fulltext query exprs", K(ret));
  } else if (OB_FAIL(exprs.push_back(field_))) {
    LOG_WARN("fail to push back field expr", K(ret));
  } else if (OB_FAIL(exprs.push_back(slop_))) {
    LOG_WARN("fail to push back slop expr", K(ret));
  }
  return ret;
}

int ObDSLMatchPhraseQuery::produce_query_node(
    const ObTableSchema &main_table_schema,
    const ObSqlSchemaGuard &schema_guard,
    ObIAllocator &alloc,
    ObFullTextQueryNode *&query_node) const
{
  int ret = OB_SUCCESS;
  ObMatchPhraseQueryNode *match_phrase_query_node = nullptr;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid match phrase query", K(ret), KPC(this));
  } else if (OB_ISNULL(match_phrase_query_node = OB_NEWx(ObMatchPhraseQueryNode, &alloc, alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObMatchPhraseQueryNode", K(ret));
  } else if (OB_FAIL(ObDSLFullTextQuery::resolve_index_with_field(
      *field_,
      main_table_schema,
      schema_guard,
      match_phrase_query_node->inv_idx_tid_,
      match_phrase_query_node->doc_id_idx_tid_,
      match_phrase_query_node->index_name_))) {
    LOG_WARN("failed to resolve fulltext index with field", K(ret));
  } else  {
    match_phrase_query_node->query_text_ = query_;
    match_phrase_query_node->boost_ = boost_;
    match_phrase_query_node->field_ = field_;
    match_phrase_query_node->slop_ = slop_;
  }

  if (OB_FAIL(ret)) {
    match_phrase_query_node->~ObMatchPhraseQueryNode();
    alloc.free(match_phrase_query_node);
    match_phrase_query_node = nullptr;
  } else {
    query_node = match_phrase_query_node;
  }
  return ret;
}

bool ObDSLMatchPhraseQuery::is_valid() const
{
  return ObDSLFullTextQuery::is_valid()
      && nullptr != field_
      && nullptr != slop_;
}

int ObDSLQueryStringQuery::create(
    ObEsQueryItem outer_query_type,
    ObDSLQuery *parent_query,
    ObIAllocator &alloc,
    ObDSLQueryStringQuery *&query_string_query)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_string_query = OB_NEWx(ObDSLQueryStringQuery, &alloc, outer_query_type, parent_query, alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObDSLQueryStringQuery", K(ret));
  }
  return ret;
}

int ObDSLQueryStringQuery::deep_copy(
    ObDSLQuery *parent,
    ObIRawExprCopier &expr_copier,
    ObIAllocator &allocator,
    ObDSLFullTextQuery *&dst) const
{
  int ret = OB_SUCCESS;
  ObDSLQueryStringQuery *tmp_dst = nullptr;
  if (OB_FAIL(ObDSLQueryStringQuery::create(outer_query_type_, parent, allocator, tmp_dst))) {
    LOG_WARN("fail to create query string query", K(ret));
  } else if (OB_FAIL(inner_deep_copy(expr_copier, *tmp_dst))) {
    LOG_WARN("fail to inner deep copy query string query", K(ret));
  } else {
    dst = tmp_dst;
  }
  return ret;
}

int ObDSLQueryStringQuery::inner_deep_copy(ObIRawExprCopier &expr_copier, ObDSLQueryStringQuery &dst) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDSLFullTextQuery::inner_deep_copy(expr_copier, dst))) {
    LOG_WARN("fail to inner deep copy fulltext query", K(ret));
  } else if (OB_FAIL(fields_param_.deep_copy(expr_copier, dst.fields_param_))) {
    LOG_WARN("fail to deep copy fields param", K(ret));
  }
  return ret;
}

int ObDSLQueryStringQuery::collect_exprs(ObIArray<ObRawExpr *> &exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDSLFullTextQuery::collect_exprs(exprs))) {
    LOG_WARN("fail to collect fulltext query exprs", K(ret));
  } else if (OB_FAIL(fields_param_.collect_exprs(exprs))) {
    LOG_WARN("fail to collect fields param exprs", K(ret));
  }
  return ret;
}

int ObDSLQueryStringQuery::produce_query_node(
    const ObTableSchema &main_table_schema,
    const ObSqlSchemaGuard &schema_guard,
    ObIAllocator &alloc,
    ObFullTextQueryNode *&query_node) const
{
  int ret = OB_SUCCESS;
  ObQueryStringQueryNode *query_string_query_node = nullptr;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid query string query", K(ret), KPC(this));
  } else if (OB_ISNULL(query_string_query_node = OB_NEWx(ObQueryStringQueryNode, &alloc, alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObQueryStringQueryNode", K(ret));
  } else {
    query_string_query_node->query_text_ = query_;
    query_string_query_node->boost_ = boost_;
    query_string_query_node->minimum_should_match_ = fields_param_.minimum_should_match_;
    query_string_query_node->default_operator_ = fields_param_.operator_;
    query_string_query_node->field_type_ = fields_param_.field_type_;
    query_string_query_node->fields_.set_allocator(&alloc);
    query_string_query_node->field_boosts_.set_allocator(&alloc);
    query_string_query_node->inv_idx_tids_.set_allocator(&alloc);
    query_string_query_node->index_infos_.set_allocator(&alloc);
    query_string_query_node->index_names_.set_allocator(&alloc);
    if (OB_FAIL(query_string_query_node->fields_.assign(fields_param_.fields_))) {
      LOG_WARN("fail to assign fields", K(ret));
    } else if (OB_FAIL(query_string_query_node->field_boosts_.assign(fields_param_.field_boosts_))) {
      LOG_WARN("fail to assign field boosts", K(ret));
    } else if (OB_FAIL(query_string_query_node->inv_idx_tids_.prepare_allocate(fields_param_.field_count()))) {
      LOG_WARN("fail to prepare allocate inv idx tids", K(ret));
    } else if (OB_FAIL(query_string_query_node->index_infos_.prepare_allocate(fields_param_.field_count()))) {
      LOG_WARN("fail to prepare allocate index infos", K(ret));
    } else if (OB_FAIL(query_string_query_node->index_names_.prepare_allocate(fields_param_.field_count()))) {
      LOG_WARN("fail to prepare allocate index names", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < fields_param_.field_count(); ++i) {
      if (OB_FAIL(ObDSLFullTextQuery::resolve_index_with_field(
          *fields_param_.fields_.at(i),
          main_table_schema,
          schema_guard,
          query_string_query_node->inv_idx_tids_.at(i),
          query_string_query_node->doc_id_idx_tid_,
          query_string_query_node->index_names_.at(i)))) {
        LOG_WARN("failed to resolve fulltext index with field", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      query_string_query_node->~ObQueryStringQueryNode();
      alloc.free(query_string_query_node);
      query_string_query_node = nullptr;
    } else {
      query_node = query_string_query_node;
    }
  }
  return ret;
}

bool ObDSLQueryStringQuery::is_valid() const
{
  return ObDSLFullTextQuery::is_valid() && fields_param_.is_valid();
}

} // namespace sql
} // namespace oceanbase
