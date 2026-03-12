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

#ifndef _OB_SQL_HYBRID_SEARCH_OB_FULLTEXT_SEARCH_QUERY_H_
#define _OB_SQL_HYBRID_SEARCH_OB_FULLTEXT_SEARCH_QUERY_H_

#include "sql/hybrid_search/ob_hybrid_search_dsl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObFullTextQueryNode;

struct ObDSLFullTextMultiFieldQueryParam
{
public:
  static constexpr ObMatchFieldsType DEFAULT_FIELD_TYPE = MATCH_BEST_FIELDS;
  ObDSLFullTextMultiFieldQueryParam(ObIAllocator &alloc);
  virtual ~ObDSLFullTextMultiFieldQueryParam() {}
  bool is_valid() const;
  int deep_copy(ObIRawExprCopier &expr_copier, ObDSLFullTextMultiFieldQueryParam &dst) const;
  int collect_exprs(ObIArray<ObRawExpr *> &exprs) const;
  int64_t field_count() const { return fields_.count(); }
  TO_STRING_KV(K_(fields), K_(field_boosts), KPC_(minimum_should_match), KPC_(operator), KPC_(field_type));
  ObFixedArray<ObColumnRefRawExpr *, ObIAllocator> fields_;
  ObFixedArray<ObConstRawExpr *, ObIAllocator> field_boosts_;
  ObConstRawExpr *minimum_should_match_;
  ObConstRawExpr *operator_;
  ObConstRawExpr *field_type_;
};

class ObDSLFullTextQuery : public ObDSLQuery
{
  friend class ObDSLResolver;
public:
  static constexpr double DEFAULT_BOOST = 1.0;
  static constexpr ObMatchOperator DEFAULT_OPERATOR = MATCH_OPERATOR_OR;
  static constexpr int32_t DEFAULT_MINIMUM_SHOULD_MATCH = 1;
public:
  virtual ~ObDSLFullTextQuery() {}
  virtual int deep_copy(
      ObDSLQuery *parent,
      ObIRawExprCopier &expr_copier,
      ObIAllocator &allocator,
      ObDSLFullTextQuery *&dst) const;
  virtual int collect_exprs(ObIArray<ObRawExpr *> &exprs) const;
  virtual int produce_query_node(
      const ObTableSchema &main_table_schema,
      const ObSqlSchemaGuard &schema_guard,
      ObIAllocator &alloc,
      ObFullTextQueryNode *&query_node) const;
  bool is_valid() const { return nullptr != query_ && nullptr != boost_; }
  // currently we assume that all fulltext index involved in hybrid search are single-column indexes
  static int resolve_index_with_field(
      const ObColumnRefRawExpr &field,
      const ObTableSchema &main_table_schema,
      const ObSqlSchemaGuard &schema_guard,
      uint64_t &inv_idx_tid,
      uint64_t &doc_id_idx_tid,
      ObString &index_name);
  INHERIT_TO_STRING_KV("ObDSLQuery", ObDSLQuery, KPC_(query), KPC_(boost));
protected:
  ObDSLFullTextQuery(ObEsQueryItem query_type, ObEsQueryItem outer_query_type, ObDSLQuery *parent_query)
    : ObDSLQuery(query_type, outer_query_type, parent_query) {}
  virtual int inner_deep_copy(ObIRawExprCopier &expr_copier, ObDSLFullTextQuery &dst) const;
  ObConstRawExpr *query_;
};

class ObDSLMatchQuery final : public ObDSLFullTextQuery
{
  friend class ObDSLResolver;
public:
  virtual ~ObDSLMatchQuery() {}
  static int create(
      ObEsQueryItem outer_query_type,
      ObDSLQuery *parent_query,
      ObIAllocator &alloc,
      ObDSLMatchQuery *&match_query);
  virtual int deep_copy(
      ObDSLQuery *parent,
      ObIRawExprCopier &expr_copier,
      ObIAllocator &allocator,
      ObDSLFullTextQuery *&dst) const override;
  virtual int collect_exprs(ObIArray<ObRawExpr *> &exprs) const override;
  virtual int produce_query_node(
      const ObTableSchema &main_table_schema,
      const ObSqlSchemaGuard &schema_guard,
      ObIAllocator &alloc,
      ObFullTextQueryNode *&query_node) const override;
  bool is_valid() const;
  INHERIT_TO_STRING_KV("ObDSLFullTextQuery", ObDSLFullTextQuery,
      KPC_(field), KPC_(minimum_should_match), KPC_(operator));
private:
  ObDSLMatchQuery(ObEsQueryItem outer_query_type, ObDSLQuery *parent_query)
    : ObDSLFullTextQuery(QUERY_ITEM_MATCH, outer_query_type, parent_query) {}
  virtual int inner_deep_copy(ObIRawExprCopier &expr_copier, ObDSLMatchQuery &dst) const;
  ObColumnRefRawExpr *field_;
  ObConstRawExpr *minimum_should_match_;
  ObConstRawExpr *operator_;
};

class ObDSLMultiMatchQuery final : public ObDSLFullTextQuery
{
  friend class ObDSLResolver;
public:
  virtual ~ObDSLMultiMatchQuery() {}
  static int create(
      ObEsQueryItem outer_query_type,
      ObDSLQuery *parent_query,
      ObIAllocator &alloc,
      ObDSLMultiMatchQuery *&multi_match_query);
  virtual int deep_copy(
      ObDSLQuery *parent,
      ObIRawExprCopier &expr_copier,
      ObIAllocator &allocator,
      ObDSLFullTextQuery *&dst) const override;
  virtual int collect_exprs(ObIArray<ObRawExpr *> &exprs) const override;
  virtual int produce_query_node(
      const ObTableSchema &main_table_schema,
      const ObSqlSchemaGuard &schema_guard,
      ObIAllocator &alloc,
      ObFullTextQueryNode *&query_node) const override;
  bool is_valid() const;
  INHERIT_TO_STRING_KV("ObDSLFullTextQuery", ObDSLFullTextQuery, K_(fields_param));
private:
  ObDSLMultiMatchQuery(ObEsQueryItem outer_query_type, ObDSLQuery *parent_query, ObIAllocator &alloc)
    : ObDSLFullTextQuery(QUERY_ITEM_MULTI_MATCH, outer_query_type, parent_query),
      fields_param_(alloc) {}
  virtual int inner_deep_copy(ObIRawExprCopier &expr_copier, ObDSLMultiMatchQuery &dst) const;
  ObDSLFullTextMultiFieldQueryParam fields_param_;
};

class ObDSLMatchPhraseQuery final : public ObDSLFullTextQuery
{
  friend class ObDSLResolver;
public:
  static constexpr int32_t DEFAULT_SLOP = 0;
  virtual ~ObDSLMatchPhraseQuery() {}
  static int create(
      ObEsQueryItem outer_query_type,
      ObDSLQuery *parent_query,
      ObIAllocator &alloc,
      ObDSLMatchPhraseQuery *&match_phrase_query);
  virtual int deep_copy(
      ObDSLQuery *parent,
      ObIRawExprCopier &expr_copier,
      ObIAllocator &allocator,
      ObDSLFullTextQuery *&dst) const override;
  virtual int collect_exprs(ObIArray<ObRawExpr *> &exprs) const override;
  virtual int produce_query_node(
      const ObTableSchema &main_table_schema,
      const ObSqlSchemaGuard &schema_guard,
      ObIAllocator &alloc,
      ObFullTextQueryNode *&query_node) const override;
  bool is_valid() const;
  INHERIT_TO_STRING_KV("ObDSLFullTextQuery", ObDSLFullTextQuery, KPC_(field), KPC_(slop));
private:
  ObDSLMatchPhraseQuery(ObEsQueryItem outer_query_type, ObDSLQuery *parent_query)
    : ObDSLFullTextQuery(QUERY_ITEM_MATCH_PHRASE, outer_query_type, parent_query) {}
  int inner_deep_copy(ObIRawExprCopier &expr_copier, ObDSLMatchPhraseQuery &dst) const;
  ObColumnRefRawExpr *field_;
  ObConstRawExpr *slop_;
};

class ObDSLQueryStringQuery final : public ObDSLFullTextQuery
{
  friend class ObDSLResolver;
public:
  virtual ~ObDSLQueryStringQuery() {}
  static int create(
      ObEsQueryItem outer_query_type,
      ObDSLQuery *parent_query,
      ObIAllocator &alloc,
      ObDSLQueryStringQuery *&query_string_query);
  virtual int deep_copy(
      ObDSLQuery *parent,
      ObIRawExprCopier &expr_copier,
      ObIAllocator &allocator,
      ObDSLFullTextQuery *&dst) const override;
  virtual int collect_exprs(ObIArray<ObRawExpr *> &exprs) const override;
  virtual int produce_query_node(
      const ObTableSchema &main_table_schema,
      const ObSqlSchemaGuard &schema_guard,
      ObIAllocator &alloc,
      ObFullTextQueryNode *&query_node) const override;
  bool is_valid() const;
  INHERIT_TO_STRING_KV("ObDSLFullTextQuery", ObDSLFullTextQuery, K_(fields_param));
private:
  ObDSLQueryStringQuery(ObEsQueryItem outer_query_type, ObDSLQuery *parent_query, ObIAllocator &alloc)
    : ObDSLFullTextQuery(QUERY_ITEM_QUERY_STRING, outer_query_type, parent_query),
      fields_param_(alloc) {}
  virtual int inner_deep_copy(ObIRawExprCopier &expr_copier, ObDSLQueryStringQuery &dst) const;
private:
  ObDSLFullTextMultiFieldQueryParam fields_param_;
};

} // namespace sql

} // namespace oceanbase

#endif
