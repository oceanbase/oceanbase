/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/dml/ob_relation_traverser.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase
{
namespace sql
{

int ObRelationTraverser::traverse(ObSelectStmt *stmt, IRelationVisitor &visitor)
{
  return traverse_stmt_relation(stmt, visitor);
}

int ObRelationTraverser::traverse_stmt_relation(ObSelectStmt *stmt,
                                                IRelationVisitor &visitor,
                                                const int64_t stmt_idx)
{
  int ret = OB_SUCCESS;
  bool is_pre = (visitor.get_visit_order() == RelationVisitOrder::PRE_ORDER);
  ObSEArray<ObRelation *, 4> clauses;
  if (OB_FAIL(stmt->get_non_empty_clauses(clauses))) {
    LOG_WARN("failed to get non-empty clauses", K(ret));
  } else {
    // stmt's input is the first non-empty clause (the data source for the pipeline)
    ObRelation *stmt_input = clauses.empty() ? NULL : clauses.at(0);
    if (is_pre && OB_FAIL(visitor.visit(*stmt, stmt_input, stmt, stmt_idx))) {
      LOG_WARN("failed to visit stmt (pre-order)", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (is_pre) {
      for (int64_t i = 0; OB_SUCC(ret) && i < clauses.count(); ++i) {
        ObRelation *input = (i + 1 < clauses.count()) ? clauses.at(i + 1) : NULL;
        if (OB_FAIL(traverse_relation(clauses.at(i), input, stmt, visitor, stmt_idx))) {
          LOG_WARN("failed to traverse clause", K(ret), K(i));
        }
      }
    } else {
      for (int64_t i = clauses.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
        ObRelation *input = (i + 1 < clauses.count()) ? clauses.at(i + 1) : NULL;
        if (OB_FAIL(traverse_relation(clauses.at(i), input, stmt, visitor, stmt_idx))) {
          LOG_WARN("failed to traverse clause", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret) && !is_pre && OB_FAIL(visitor.visit(*stmt, stmt_input, stmt, stmt_idx))) {
      LOG_WARN("failed to visit stmt (post-order)", K(ret));
    }
  }
  LOG_TRACE("traverse stmt relation", K(ret), K(is_pre), K(stmt_idx), KPC(stmt));
  return ret;
}

int ObRelationTraverser::traverse_relation(ObRelation *relation, ObRelation *input,
                                            ObSelectStmt *stmt, IRelationVisitor &visitor,
                                            const int64_t stmt_idx)
{
  int ret = OB_SUCCESS;
  bool is_pre = (visitor.get_visit_order() == RelationVisitOrder::PRE_ORDER);
  if (OB_ISNULL(relation) || OB_UNLIKELY(relation->is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relation is unexpected null or empty", K(ret), KPC(relation));
  } else if (is_pre && OB_FAIL(visitor.visit(*relation, input, stmt, stmt_idx))) {
    LOG_WARN("failed to visit relation (pre-order)", K(ret), KPC(relation));
  } else if (relation->is_multi_input()) {
    ObSEArray<ObRelation *, 4> inputs;
    if (OB_NOT_NULL(input)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("multi-input relation should not have a non-null input", K(ret));
    } else if (OB_FAIL(relation->get_input_relations(stmt, inputs))) {
      LOG_WARN("failed to get input relations", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < inputs.count(); ++i) {
      ObRelation *child_relation = inputs.at(i);
      if (OB_ISNULL(child_relation)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input relation is unexpected null", K(ret), K(i));
      } else if (RelationType::SET == relation->get_relation_type()) {
        if (RelationType::SELECT != child_relation->get_relation_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("set input is not a select relation", K(ret), K(i), KPC(child_relation));
        } else if (OB_FAIL(traverse_stmt_relation(static_cast<ObSelectStmt *>(child_relation), visitor, i))) {
          LOG_WARN("failed to traverse set query branch", K(ret), K(i));
        }
      } else if (OB_FAIL(traverse_relation(child_relation, NULL, stmt, visitor, stmt_idx))) {
        LOG_WARN("failed to traverse input relation", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret) && is_recursive_ && relation->get_relation_type() == RelationType::TABLE) {
    TableItem *table_item = static_cast<TableItem *>(relation);
    if (table_item->is_generated_table() || table_item->is_temp_table()) {
      if (OB_ISNULL(table_item->ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ref_query is unexpected null", K(ret));
      } else if (OB_FAIL(traverse_stmt_relation(table_item->ref_query_, visitor))) {
        LOG_WARN("failed to traverse ref_query", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !is_pre && OB_FAIL(visitor.visit(*relation, input, stmt, stmt_idx))) {
    LOG_WARN("failed to visit relation (post-order)", K(ret), KPC(relation));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
