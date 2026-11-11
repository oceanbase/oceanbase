/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_RELATION_GROUP_H_
#define OCEANBASE_SQL_RESOLVER_DML_OB_RELATION_GROUP_H_

#include "sql/resolver/dml/ob_relation.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_copier.h"

namespace oceanbase
{
namespace sql
{

struct ObGroupbyExpr
{
  ObGroupbyExpr(common::ObIAllocator &allocator)
    : groupby_exprs_(allocator)
  {}
  int assign(const ObGroupbyExpr &other)
  { return groupby_exprs_.assign(other.groupby_exprs_); }

  TO_STRING_KV("grouping sets groupby expr", groupby_exprs_);
  ObSqlArray<ObRawExpr*> groupby_exprs_;
};

struct ObRollupItem
{
  ObRollupItem(common::ObIAllocator &allocator)
  : rollup_list_exprs_(allocator)
  {}
  int assign(const ObRollupItem &other)
  { return rollup_list_exprs_.assign(other.rollup_list_exprs_); }
  int deep_copy(ObIRawExprCopier &expr_copier,
                const ObRollupItem &other);
  TO_STRING_KV("rollup list exprs", rollup_list_exprs_);
  ObSqlArray<ObGroupbyExpr, true> rollup_list_exprs_;
};

struct ObCubeItem
{
  ObCubeItem(common::ObIAllocator &allocator) : cube_list_exprs_(allocator) { }
  int assign(const ObCubeItem &other)
  { return cube_list_exprs_.assign(other.cube_list_exprs_); }
  int deep_copy(ObIRawExprCopier &expr_copier,
                const ObCubeItem &other);
  TO_STRING_KV("cube list exprs", cube_list_exprs_);
  ObSqlArray<ObGroupbyExpr, true> cube_list_exprs_;
};

struct ObGroupingSetsItem
{
  ObGroupingSetsItem(common::ObIAllocator &allocator)
  : grouping_sets_exprs_(allocator),
    rollup_items_(allocator),
    cube_items_(allocator),
    pruned_grouping_set_ids_(allocator)
  {}
  int assign(const ObGroupingSetsItem &other);
  int deep_copy(ObIRawExprCopier &expr_copier,
                const ObGroupingSetsItem &other);
  TO_STRING_KV("grouping sets exprs", grouping_sets_exprs_, K_(rollup_items), K_(cube_items),
               K_(pruned_grouping_set_ids));
  ObSqlArray<ObGroupbyExpr, true> grouping_sets_exprs_;
  ObSqlArray<ObRollupItem, true> rollup_items_;
  ObSqlArray<ObCubeItem, true> cube_items_;
  // select sum(a) from t group by grouping sets(b, c) having b > 1
  // `grouping_sets_exprs_` will contain '[b], [c]'
  // `pruned_grouping_sets_exprs_` will contain `[1]`
  ObSqlArray<int64_t> pruned_grouping_set_ids_;
};

class ObGroupRelation : public ObRelation
{
public:
  ObGroupRelation(common::ObIAllocator &allocator)
    : ObRelation(allocator),
      group_exprs_(allocator),
      rollup_exprs_(allocator),
      having_exprs_(allocator),
      agg_items_(allocator),
      rollup_directions_(allocator),
      grouping_sets_items_(allocator),
      rollup_items_(allocator),
      cube_items_(allocator)
  {}
  virtual ~ObGroupRelation() {}

  int assign(const ObGroupRelation &other);
  int deep_copy(ObRawExprCopier &expr_copier,
                const ObGroupRelation &other);

  virtual RelationType get_relation_type() const override { return RelationType::GROUP; }
  bool is_empty() const override;

public:
  ObSqlArray<ObRawExpr *> group_exprs_;
  ObSqlArray<ObRawExpr *> rollup_exprs_;
  ObSqlArray<ObRawExpr *> having_exprs_;
  ObSqlArray<ObAggFunRawExpr *> agg_items_;
  //这个结构仅仅是在mysql模式下的rollup语句中才有意义。比如下面这条语句
  //select a,b,sum(d) from t group by a desc, b asc with rollup.
  ObSqlArray<ObOrderDirection> rollup_directions_;
  ObSqlArray<ObGroupingSetsItem, true> grouping_sets_items_;
  ObSqlArray<ObRollupItem, true> rollup_items_;
  ObSqlArray<ObCubeItem, true> cube_items_;

  INHERIT_TO_STRING_KV("ObRelation", ObRelation,
                       K_(group_exprs),
                       K_(rollup_exprs),
                       K_(having_exprs),
                       K_(agg_items));
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESOLVER_DML_OB_RELATION_GROUP_H_
