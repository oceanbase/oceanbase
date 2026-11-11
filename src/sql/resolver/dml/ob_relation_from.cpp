/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/dml/ob_relation_from.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase
{
namespace sql
{

int ObFromRelation::assign(const ObFromRelation &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(from_items_.assign(other.from_items_))) {
    LOG_WARN("failed to assign from_items", K(ret));
  } else if (OB_FAIL(condition_exprs_.assign(other.condition_exprs_))) {
    LOG_WARN("failed to assign condition_exprs", K(ret));
  } else if (OB_FAIL(semi_infos_.assign(other.semi_infos_))) {
    LOG_WARN("failed to assign semi_infos", K(ret));
  }
  return ret;
}

int ObFromRelation::deep_copy(ObIAllocator &allocator,
                              ObRawExprCopier &expr_copier,
                              const ObFromRelation &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(from_items_.assign(other.from_items_))) {
    LOG_WARN("failed to assign from items", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.condition_exprs_, condition_exprs_))) {
    LOG_WARN("failed to copy condition exprs", K(ret));
  } else if (OB_FAIL(deep_copy_stmt_objects<SemiInfo>(allocator,
                                                      expr_copier,
                                                      other.semi_infos_,
                                                      semi_infos_))) {
    LOG_WARN("failed to deep copy semi infos", K(ret));
  }
  return ret;
}

bool ObFromRelation::is_empty() const
{
  // from relation is always not empty, even though it has no from items
  return false;
}

int ObFromRelation::get_input_relations(ObSelectStmt *stmt, ObIArray<ObRelation *> &inputs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is unexpected null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < from_items_.count(); ++i) {
    TableItem *table_item = stmt->get_table_item(from_items_.at(i));
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is unexpected null", K(ret));
    } else if (OB_FAIL(inputs.push_back(table_item))) {
      LOG_WARN("failed to push back input relation", K(ret));
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
