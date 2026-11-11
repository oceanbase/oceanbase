/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_RELATION_FROM_H_
#define OCEANBASE_SQL_RESOLVER_DML_OB_RELATION_FROM_H_

#include "sql/resolver/dml/ob_relation.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_copier.h"

namespace oceanbase
{
namespace sql
{

struct FromItem
{
  FromItem()
    : table_id_(common::OB_INVALID_ID),
      link_table_id_(common::OB_INVALID_ID),
      is_joined_(false)
  {}
  uint64_t table_id_;
  uint64_t link_table_id_;
  // false: it is the real table id
  // true: it is the joined table id
  bool is_joined_;

  bool operator ==(const FromItem &r) const
  { return table_id_ == r.table_id_ && is_joined_ == r.is_joined_; }

  int deep_copy(const FromItem &other);
  TO_STRING_KV(N_TID, table_id_, N_IS_JOIN, is_joined_);
};

class SemiInfo
{
public:
  SemiInfo(ObIAllocator &allocator, ObJoinType join_type = LEFT_SEMI_JOIN)
  : join_type_(join_type),
    semi_id_(common::OB_INVALID_ID),
    right_table_id_(common::OB_INVALID_ID),
    left_table_ids_(allocator),
    semi_conditions_(allocator)
  {}
  virtual ~SemiInfo() {}
  int deep_copy(ObIRawExprCopier &copier, const SemiInfo &other);
  inline bool is_semi_join() const
  { return LEFT_SEMI_JOIN == join_type_ || RIGHT_SEMI_JOIN == join_type_; }
  inline bool is_anti_join() const
  { return LEFT_ANTI_JOIN == join_type_ || RIGHT_ANTI_JOIN == join_type_; }
  TO_STRING_KV(K_(join_type),
               K_(semi_id),
               K_(left_table_ids),
               K_(right_table_id),
               K_(semi_conditions));

  ObJoinType join_type_;
  uint64_t semi_id_;
  //generate table create from subquery
  uint64_t right_table_id_;
  //table ids which involved right table in parent query
  ObSqlArray<uint64_t> left_table_ids_;
  ObSqlArray<ObRawExpr*> semi_conditions_;
};

class ObFromRelation : public ObRelation
{
public:
  ObFromRelation(common::ObIAllocator &allocator)
    : ObRelation(allocator),
      from_items_(allocator),
      condition_exprs_(allocator),
      semi_infos_(allocator)
  {}
  virtual ~ObFromRelation() {}

  int assign(const ObFromRelation &other);
  int deep_copy(ObIAllocator &allocator,
                ObRawExprCopier &expr_copier,
                const ObFromRelation &other);

  virtual RelationType get_relation_type() const override { return RelationType::FROM; }
  bool is_empty() const override;
  bool is_multi_input() const override { return true; }
  int get_input_relations(ObSelectStmt *stmt, ObIArray<ObRelation *> &inputs) override;

public:
  ObSqlArray<FromItem> from_items_;
  ObSqlArray<ObRawExpr *> condition_exprs_;
  ObSqlArray<SemiInfo *> semi_infos_;

  INHERIT_TO_STRING_KV("ObRelation", ObRelation,
                       K_(from_items),
                       K_(condition_exprs),
                       K_(semi_infos));
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESOLVER_DML_OB_RELATION_FROM_H_
