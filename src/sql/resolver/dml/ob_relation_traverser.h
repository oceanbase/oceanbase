/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_RELATION_TRAVERSER_H_
#define OCEANBASE_SQL_RESOLVER_DML_OB_RELATION_TRAVERSER_H_

#include "sql/resolver/dml/ob_relation.h"

namespace oceanbase
{
namespace sql
{

enum class RelationVisitOrder
{
  PRE_ORDER = 0,
  POST_ORDER = 1
};

class IRelationVisitor
{
public:
  virtual ~IRelationVisitor() {}
  virtual RelationVisitOrder get_visit_order() const = 0;
  virtual int visit(ObRelation &relation,
                    ObRelation *input,
                    ObSelectStmt *stmt,
                    const int64_t stmt_idx) = 0;
};

class ObRelationTraverser
{
public:
  explicit ObRelationTraverser(bool is_recursive = false) : is_recursive_(is_recursive) {}

  int traverse(ObSelectStmt *stmt, IRelationVisitor &visitor);

private:
  int traverse_stmt_relation(ObSelectStmt *stmt,
                             IRelationVisitor &visitor,
                             const int64_t stmt_idx = OB_INVALID_INDEX);
  int traverse_relation(ObRelation *relation, ObRelation *input,
                        ObSelectStmt *stmt, IRelationVisitor &visitor,
                        const int64_t stmt_idx);

  bool is_recursive_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESOLVER_DML_OB_RELATION_TRAVERSER_H_
